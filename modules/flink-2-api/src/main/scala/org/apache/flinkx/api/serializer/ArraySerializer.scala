package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.reflect.ClassTag

class ArraySerializer[T](val child: TypeSerializer[T], clazz: Class[T]) extends SimpleSerializer[Array[T]] {
  implicit val classTag: ClassTag[T]      = ClassTag(clazz)
  override def createInstance(): Array[T] = Array.empty[T]
  override def getLength: Int             = -1
  override def deserialize(source: DataInputView): Array[T] = {
    val count = source.readInt()
    val result = for {
      _ <- 0 until count
    } yield {
      child.deserialize(source)
    }
    result.toArray
  }
  override def serialize(record: Array[T], target: DataOutputView): Unit = {
    target.writeInt(record.length)
    record.foreach(element => child.serialize(element, target))
  }
  override def snapshotConfiguration(): TypeSerializerSnapshot[Array[T]] =
    new CollectionSerializerSnapshot[Array, T, ArraySerializer[T]](child, classOf[ArraySerializer[T]], clazz)
}
