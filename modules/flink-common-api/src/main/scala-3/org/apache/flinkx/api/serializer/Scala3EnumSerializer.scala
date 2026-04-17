package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.setNestedSerializersSnapshots
import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer
import org.apache.flink.api.common.typeutils.{CompositeTypeSerializerSnapshot, TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarkerByte, VariableLengthDataType}

/** Serializer for Scala 3 enum. Handle nullable value. */
class Scala3EnumSerializer[T <: Product](
    val enumValueNames: Array[String],
    val enumValueSerializers: Array[TypeSerializer[_]]
) extends MutableSerializer[T] {

  override val isImmutableType: Boolean = enumValueSerializers.forall(_.isImmutableType)
  val isImmutableSerializer: Boolean    = enumValueSerializers.forall(s => s.duplicate().eq(s))

  override def copy(from: T): T = {
    if (from == null || isImmutableType) {
      from
    } else {
      val i = enumValueNames.indexOf(from.productPrefix) // productPrefix returns enum value name even for case class
      enumValueSerializers(i).asInstanceOf[TypeSerializer[T]].copy(from)
    }
  }

  override def duplicate(): Scala3EnumSerializer[T] = {
    if (isImmutableSerializer) {
      this
    } else {
      new Scala3EnumSerializer[T](enumValueNames, enumValueSerializers.map(_.duplicate()))
    }
  }

  override def createInstance(): T =
    enumValueSerializers.head.createInstance().asInstanceOf[T]

  override val getLength: Int = {
    val length = enumValueSerializers(0).getLength
    if (enumValueSerializers.forall(_.getLength == length)) {
      length
    } else {
      VariableLengthDataType
    }
  }

  override def serialize(record: T, target: DataOutputView): Unit = {
    if (record == null) {
      target.writeByte(NullMarkerByte)
    } else {
      val enumValueIndex = enumValueNames.indexOf(record.productPrefix) // returns enum value name even for case class
      if (enumValueIndex >= 0) {
        target.writeByte(enumValueIndex)
        enumValueSerializers(enumValueIndex).asInstanceOf[TypeSerializer[T]].serialize(record, target)
      } else {
        throw new IllegalStateException("enum value not found in enum schema")
      }
    }
  }

  override def deserialize(source: DataInputView): T = {
    val index = source.readByte()
    if (index == NullMarkerByte) {
      null.asInstanceOf[T]
    } else {
      val subtype = enumValueSerializers(index.toInt)
      subtype.asInstanceOf[TypeSerializer[T]].deserialize(source)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val index   = source.readByte()
    target.writeByte(index)
    if (index != NullMarkerByte) {
      val subtype = enumValueSerializers(index.toInt)
      subtype.asInstanceOf[TypeSerializer[T]].copy(source, target)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] = new Scala3EnumSerializerSnapshot(Some(this))

}

/** Serializer snapshot for Scala 3 enum. */
class Scala3EnumSerializerSnapshot[T <: Product](
    serializer: Option[Scala3EnumSerializer[T]]
) extends CompositeTypeSerializerSnapshot[T, Scala3EnumSerializer[T]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(None)

  private var enumValueNames: Array[String] = Array.empty

  serializer.foreach { s =>
    // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
    setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
    enumValueNames = s.enumValueNames
  }

  override def getCurrentOuterSnapshotVersion: Int = Scala3EnumSerializerSnapshot.CurrentVersion

  override protected def getNestedSerializers(outerSerializer: Scala3EnumSerializer[T]): Array[TypeSerializer[_]] =
    outerSerializer.enumValueSerializers

  override protected def createOuterSerializerWithNestedSerializers(
      nestedSerializers: Array[TypeSerializer[_]]
  ): Scala3EnumSerializer[T] =
    new Scala3EnumSerializer(enumValueNames, nestedSerializers)

  override def writeOuterSnapshot(out: DataOutputView): Unit =
    StringArraySerializer.INSTANCE.serialize(enumValueNames, out)

  override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
    enumValueNames = StringArraySerializer.INSTANCE.deserialize(in)
  }

}

object Scala3EnumSerializerSnapshot {
  private val CurrentVersion = 1
}
