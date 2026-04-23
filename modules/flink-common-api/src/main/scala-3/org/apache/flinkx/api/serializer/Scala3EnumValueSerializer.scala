package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil

/** Serializer for Scala 3 enum value. */
class Scala3EnumValueSerializer[T](companionClass: Class[_], enumValueName: String) extends ImmutableSerializer[T] {

  private lazy val enumValue: T = companionClass.getField(enumValueName).get(null).asInstanceOf[T]

  override def copy(source: DataInputView, target: DataOutputView): Unit = {}
  override def createInstance(): T                                       = enumValue
  override def getLength: Int                                            = 0
  override def serialize(record: T, target: DataOutputView): Unit        = {}
  override def deserialize(source: DataInputView): T                     = enumValue

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new Scala3EnumValueSerializerSnapshot(companionClass, enumValueName)
}

/** Serializer snapshot for Scala 3 enum value. */
class Scala3EnumValueSerializerSnapshot[T](
    var companionClass: Class[_],
    var enumValueName: String
) extends TypeSerializerSnapshot[T] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(null, null)

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    companionClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader)
    enumValueName = in.readUTF()
  }

  override def writeSnapshot(out: DataOutputView): Unit = {
    out.writeUTF(companionClass.getName)
    out.writeUTF(enumValueName)
  }

  override def getCurrentVersion: Int = Scala3EnumValueSerializerSnapshot.CurrentVersion

  override def resolveSchemaCompatibility(
      oldSerializer: TypeSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] =
    TypeSerializerSchemaCompatibility.compatibleAsIs()

  override def restoreSerializer(): TypeSerializer[T] = new Scala3EnumValueSerializer[T](companionClass, enumValueName)

}

object Scala3EnumValueSerializerSnapshot {
  private val CurrentVersion = 1
}
