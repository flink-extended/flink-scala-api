package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class SetSerializer[T](child: TypeSerializer[T], clazz: Class[T]) extends MutableSerializer[Set[T]] {

  override val isImmutableType: Boolean = child.isImmutableType

  override def copy(from: Set[T]): Set[T] = {
    if (from == null || isImmutableType) {
      from
    } else {
      from.map(child.copy)
    }
  }

  override def createInstance(): Set[T] = Set.empty[T]
  override def getLength: Int           = -1
  override def deserialize(source: DataInputView): Set[T] = {
    val count = source.readInt()
    val result = for {
      _ <- 0 until count
    } yield {
      child.deserialize(source)
    }
    result.toSet
  }
  override def serialize(record: Set[T], target: DataOutputView): Unit = {
    target.writeInt(record.size)
    record.foreach(element => child.serialize(element, target))
  }
  override def snapshotConfiguration(): TypeSerializerSnapshot[Set[T]] =
    new CollectionSerializerSnapshot[Set, T, SetSerializer[T]](child, classOf[SetSerializer[T]], clazz)

}
