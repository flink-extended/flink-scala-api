package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.serializer.MappedSerializer.{MappedSerializerSnapshot, TypeMapper}

case class MappedSerializer[A, B](mapper: TypeMapper[A, B], ser: TypeSerializer[B]) extends SimpleSerializer[A] {
  override def equals(obj: Any): Boolean = ser.equals(obj)

  override def toString: String = ser.toString

  override def hashCode(): Int = ser.hashCode()
  override def getLength: Int  = ser.getLength

  override def serialize(record: A, target: DataOutputView): Unit = {
    ser.serialize(mapper.map(record), target)
  }

  override def deserialize(reuse: A, source: DataInputView): A = {
    mapper.contramap(ser.deserialize(mapper.map(reuse), source))
  }

  override def deserialize(source: DataInputView): A = mapper.contramap(ser.deserialize(source))

  override def snapshotConfiguration(): TypeSerializerSnapshot[A] = new MappedSerializerSnapshot[A, B](mapper, ser)

  override def createInstance(): A = mapper.contramap(ser.createInstance())
}

object MappedSerializer {
  trait TypeMapper[A, B] extends Serializable {
    def map(a: A): B
    def contramap(b: B): A
  }

  class MappedSerializerSnapshot[A, B](
      var mapper: TypeMapper[A, B],
      var ser: TypeSerializer[B]
  ) extends TypeSerializerSnapshot[A] {

    // Empty constructor is required to instantiate this class during deserialization.
    def this() = this(null, null)

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
      val mapperClazz = InstantiationUtil.resolveClassByName[TypeMapper[A, B]](in, userCodeClassLoader)
      mapper = InstantiationUtil.instantiate(mapperClazz)
      val serClazz = InstantiationUtil.resolveClassByName[TypeSerializer[B]](in, userCodeClassLoader)
      ser = TypeSerializerSnapshot.readVersionedSnapshot[B](in, userCodeClassLoader).restoreSerializer()
    }

    override def resolveSchemaCompatibility(
        oldSerializerSnapshot: TypeSerializerSnapshot[A]
    ): TypeSerializerSchemaCompatibility[A] =
      TypeSerializerSchemaCompatibility.compatibleAsIs()

    override def writeSnapshot(out: DataOutputView): Unit = {
      out.writeUTF(mapper.getClass.getName)
      TypeSerializerSnapshot.writeVersionedSnapshot(out, ser.snapshotConfiguration())
    }

    override def restoreSerializer(): TypeSerializer[A] = new MappedSerializer[A, B](mapper, ser)

    override def getCurrentVersion: Int = 2
  }

}
