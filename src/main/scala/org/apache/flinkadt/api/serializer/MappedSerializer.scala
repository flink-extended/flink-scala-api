package org.apache.flinkadt.api.serializer

import org.apache.flinkadt.api.serializer.MappedSerializer.{MappedSerializerSnapshot, TypeMapper}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{
  CompositeTypeSerializerSnapshot,
  GenericTypeSerializerSnapshot,
  SimpleTypeSerializerSnapshot,
  TypeSerializer,
  TypeSerializerSchemaCompatibility,
  TypeSerializerSnapshot
}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil

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
  trait TypeMapper[A, B] {
    def map(a: A): B
    def contramap(b: B): A
  }
  class MappedSerializerSnapshot[A, B]() extends TypeSerializerSnapshot[A] {
    var mapper: TypeMapper[A, B] = _
    var ser: TypeSerializer[B]   = _
    def this(xmapper: TypeMapper[A, B], xser: TypeSerializer[B]) = {
      this()
      mapper = xmapper
      ser = xser
    }

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
      val mapperClazz = InstantiationUtil.resolveClassByName[TypeMapper[A, B]](in, userCodeClassLoader)
      mapper = InstantiationUtil.instantiate(mapperClazz)
      val serClazz = InstantiationUtil.resolveClassByName(in, userCodeClassLoader)
      ser = InstantiationUtil.instantiate(serClazz)
    }

    override def resolveSchemaCompatibility(newSerializer: TypeSerializer[A]): TypeSerializerSchemaCompatibility[A] =
      TypeSerializerSchemaCompatibility.compatibleAsIs()

    override def writeSnapshot(out: DataOutputView): Unit = {
      out.writeUTF(mapper.getClass.getName)
      out.writeUTF(ser.getClass.getName)
    }

    override def restoreSerializer(): TypeSerializer[A] = new MappedSerializer[A, B](mapper, ser)

    override def getCurrentVersion: Int = 1
  }
}
