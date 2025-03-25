package org.apache.flinkx.api.serializer

import org.apache.flinkx.api.serializer.ScalaCaseObjectSerializer.ScalaCaseObjectSerializerSnapshot
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil

class ScalaCaseObjectSerializer[T](clazz: Class[T]) extends TypeSerializerSingleton[T] {
  override def isImmutableType: Boolean                                  = true
  override def copy(from: T): T                                          = from
  override def copy(from: T, reuse: T): T                                = from
  override def copy(source: DataInputView, target: DataOutputView): Unit = {}
  override def createInstance(): T                                = clazz.getField("MODULE$").get(null).asInstanceOf[T]
  override def getLength: Int                                     = 0
  override def serialize(record: T, target: DataOutputView): Unit = {}

  override def deserialize(source: DataInputView): T = {
    clazz.getField("MODULE$").get(null).asInstanceOf[T]
  }
  override def deserialize(reuse: T, source: DataInputView): T = deserialize(source)
  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new ScalaCaseObjectSerializerSnapshot(clazz)
}

object ScalaCaseObjectSerializer {
  class ScalaCaseObjectSerializerSnapshot[T](var clazz: Class[T]) extends TypeSerializerSnapshot[T] {
    def this() = this(null)

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
      clazz = InstantiationUtil.resolveClassByName(in, userCodeClassLoader)
    }

    override def writeSnapshot(out: DataOutputView): Unit = {
      out.writeUTF(clazz.getName)
    }

    override def getCurrentVersion: Int = 1
    override def resolveSchemaCompatibility(newSerializer: TypeSerializer[T]): TypeSerializerSchemaCompatibility[T] =
      TypeSerializerSchemaCompatibility.compatibleAsIs()

    override def restoreSerializer(): TypeSerializer[T] =
      new ScalaCaseObjectSerializer[T](clazz)

  }
}
