package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class VectorSerializer[T](child: TypeSerializer[T], clazz: Class[T]) extends SimpleSerializer[Vector[T]] {
  override def createInstance(): Vector[T] = Vector.empty[T]
  override def getLength: Int              = -1
  override def deserialize(source: DataInputView): Vector[T] = {
    val count = source.readInt()
    val result = for {
      _ <- 0 until count
    } yield {
      child.deserialize(source)
    }
    result.toVector
  }
  override def serialize(record: Vector[T], target: DataOutputView): Unit = {
    target.writeInt(record.size)
    record.foreach(element => child.serialize(element, target))
  }
  override def snapshotConfiguration(): TypeSerializerSnapshot[Vector[T]] =
    new CollectionSerializerSnapshot[Vector, T, VectorSerializer[T]](child, classOf[VectorSerializer[T]], clazz)

}
