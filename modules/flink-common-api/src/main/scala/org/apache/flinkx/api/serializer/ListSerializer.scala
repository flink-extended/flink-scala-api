package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class ListSerializer[T](child: TypeSerializer[T], clazz: Class[T]) extends MutableSerializer[List[T]] {

  override val isImmutableType: Boolean = child.isImmutableType

  override def copy(from: List[T]): List[T] = {
    if (from == null || isImmutableType) {
      from
    } else {
      from.map(child.copy)
    }
  }

  override def duplicate(): ListSerializer[T] = {
    val duplicatedChild = child.duplicate()
    if (duplicatedChild.eq(child)) {
      this
    } else {
      new ListSerializer[T](duplicatedChild, clazz)
    }
  }

  override def createInstance(): List[T]                   = List.empty[T]
  override def getLength: Int                              = -1
  override def deserialize(source: DataInputView): List[T] = {
    val count  = source.readInt()
    val result = for {
      _ <- 0 until count
    } yield {
      child.deserialize(source)
    }
    result.toList
  }
  override def serialize(record: List[T], target: DataOutputView): Unit = {
    target.writeInt(record.size)
    record.foreach(element => child.serialize(element, target))
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[List[T]] =
    new CollectionSerializerSnapshot[List, T, ListSerializer[T]](child, classOf[ListSerializer[T]], clazz)

}
