package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.setNestedSerializersSnapshots
import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer
import org.apache.flink.api.common.typeutils.{
  CompositeTypeSerializerSnapshot,
  TypeSerializer,
  TypeSerializerSchemaCompatibility,
  TypeSerializerSnapshot
}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.VariableLengthDataType
import org.apache.flinkx.api.serializer.CoproductSerializer.CoproductSerializerSnapshot
import org.apache.flinkx.api.util.ClassUtil

class CoproductSerializer[T](val subtypeClasses: Array[Class[_]], val subtypeSerializers: Array[TypeSerializer[_]])
    extends MutableSerializer[T] {

  override val isImmutableType: Boolean = subtypeSerializers.forall(_.isImmutableType)
  val isImmutableSerializer: Boolean    = subtypeSerializers.forall(s => s.duplicate().eq(s))

  override def copy(from: T): T = {
    if (from == null || isImmutableType) {
      from
    } else {
      val i = subtypeClasses.indexWhere(_.isInstance(from))
      subtypeSerializers(i).asInstanceOf[TypeSerializer[T]].copy(from)
    }
  }

  override def duplicate(): CoproductSerializer[T] = {
    if (isImmutableSerializer) {
      this
    } else {
      new CoproductSerializer[T](subtypeClasses, subtypeSerializers.map(_.duplicate()))
    }
  }

  override def createInstance(): T =
    // this one may be used for later reuse, but we never reuse coproducts due to their unclear concrete type
    subtypeSerializers.head.createInstance().asInstanceOf[T]

  override val getLength: Int = {
    val length = subtypeSerializers(0).getLength
    if (subtypeSerializers.forall(_.getLength == length)) {
      length
    } else {
      VariableLengthDataType
    }
  }

  override def serialize(record: T, target: DataOutputView): Unit = {
    var subtypeIndex = 0
    var found        = false
    while (!found && (subtypeIndex < subtypeClasses.length)) {
      if (subtypeClasses(subtypeIndex).isInstance(record)) {
        found = true
      } else {
        subtypeIndex += 1
      }
    }
    if (found) {
      target.writeByte(subtypeIndex.toByte.toInt)
      subtypeSerializers(subtypeIndex).asInstanceOf[TypeSerializer[T]].serialize(record, target)
    } else {
      throw new IllegalStateException("subtype not found in sealed trait schema")
    }
  }

  override def deserialize(source: DataInputView): T = {
    val index   = source.readByte()
    val subtype = subtypeSerializers(index.toInt)
    subtype.asInstanceOf[TypeSerializer[T]].deserialize(source)
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val index   = source.readByte()
    val subtype = subtypeSerializers(index.toInt)
    target.writeByte(index)
    subtype.asInstanceOf[TypeSerializer[T]].copy(source, target)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new CoproductSerializerSnapshot(Some(this))
}

object CoproductSerializer {

  private val CurrentVersion = 3

  class CoproductSerializerSnapshot[T](
      serializer: Option[CoproductSerializer[T]]
  ) extends TypeSerializerSnapshot[T] {

    // Empty constructor is required to instantiate this class during deserialization.
    def this() = this(None)

    private var subtypeClasses: Array[Class[_]]              = Array.empty
    private var subtypeSerializers: Array[TypeSerializer[_]] = Array.empty

    // An adapter is mandatory to keep the compatibility during the transition to a CompositeTypeSerializerSnapshot
    // because its readSnapshot() method is final
    private val adapter: CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] =
      new CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] {

        serializer.foreach { s =>
          // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
          subtypeClasses = s.subtypeClasses
          subtypeSerializers = s.subtypeSerializers
          setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
        }

        override def getCurrentOuterSnapshotVersion: Int = CurrentVersion

        override def getNestedSerializers(outerSerializer: CoproductSerializer[T]): Array[TypeSerializer[_]] =
          subtypeSerializers

        override def createOuterSerializerWithNestedSerializers(
            nestedSerializers: Array[TypeSerializer[_]]
        ): CoproductSerializer[T] =
          new CoproductSerializer[T](subtypeClasses, nestedSerializers)

        override def writeOuterSnapshot(out: DataOutputView): Unit =
          StringArraySerializer.INSTANCE.serialize(subtypeClasses.map(_.getName), out)

        override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
          subtypeClasses = StringArraySerializer.INSTANCE.deserialize(in).map(ClassUtil.resolveClassByName(_, cl))
        }

      }

    override def getCurrentVersion: Int = adapter.getCurrentVersion

    override def writeSnapshot(out: DataOutputView): Unit = adapter.writeSnapshot(out)

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit =
      if (readVersion == 2) {
        val len = in.readInt()

        subtypeClasses = (0 until len)
          .map(_ => InstantiationUtil.resolveClassByName(in, userCodeClassLoader))
          .toArray

        subtypeSerializers = (0 until len)
          .map(_ => TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader).restoreSerializer())
          .toArray
      } else {
        adapter.readSnapshot(readVersion, in, userCodeClassLoader)
      }

    override def resolveSchemaCompatibility(
        oldSerializerSnapshot: TypeSerializerSnapshot[T]
    ): TypeSerializerSchemaCompatibility[T] = adapter.resolveSchemaCompatibility(oldSerializerSnapshot)

    override def restoreSerializer(): TypeSerializer[T] =
      if (subtypeSerializers.isEmpty) {
        adapter.restoreSerializer() // Restore from adapter
      } else {
        new CoproductSerializer[T](subtypeClasses, subtypeSerializers) // Restore from readSnapshot
      }

  }

}
