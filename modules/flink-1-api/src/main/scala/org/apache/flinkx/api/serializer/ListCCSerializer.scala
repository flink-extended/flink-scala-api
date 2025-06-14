package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class ListCCSerializer[T](child: TypeSerializer[T], clazz: Class[T]) extends MutableSerializer[::[T]] {

  override val isImmutableType: Boolean = child.isImmutableType

  override def copy(from: ::[T]): ::[T] = {
    if (from == null || isImmutableType) {
      from
    } else {
      val result = from.map(child.copy)
      ::(result.head, result.tail)
    }
  }

  override def duplicate(): ListCCSerializer[T] = {
    val duplicatedChild = child.duplicate()
    if (duplicatedChild.eq(child)) {
      this
    } else {
      new ListCCSerializer[T](duplicatedChild, clazz)
    }
  }

  override def createInstance(): ::[T] = throw new IllegalArgumentException("cannot create instance of non-empty list")
  override def getLength: Int          = -1
  override def deserialize(source: DataInputView): ::[T] = {
    val count  = source.readInt()
    val result = (0 until count)
      .map(_ => child.deserialize(source))

    ::(result.head, result.tail.toList)
  }
  override def serialize(record: ::[T], target: DataOutputView): Unit = {
    target.writeInt(record.size)
    record.foreach(element => child.serialize(element, target))
  }
  override def snapshotConfiguration(): TypeSerializerSnapshot[::[T]] =
    new CollectionSerializerSnapshot[::, T, ListCCSerializer[T]](child, classOf[ListCCSerializer[T]], clazz)

}
