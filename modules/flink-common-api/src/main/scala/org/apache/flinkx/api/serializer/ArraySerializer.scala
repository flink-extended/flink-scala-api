package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.reflect.ClassTag

class ArraySerializer[T](val child: TypeSerializer[T], clazz: Class[T]) extends MutableSerializer[Array[T]] {

  implicit val classTag: ClassTag[T] = ClassTag(clazz)

  override def createInstance(): Array[T] = Array.empty[T]

  override def copy(from: Array[T]): Array[T] = {
    if (from == null) {
      from
    } else {
      val length = from.length
      val copy   = Array.copyOf(from, length)
      if (!child.isImmutableType) {
        var i = 0
        while (i < length) {
          val element = copy(i)
          if (element != null) copy(i) = child.copy(element)
          i += 1
        }
      }
      copy
    }
  }

  override def duplicate(): ArraySerializer[T] = {
    val duplicatedChild = child.duplicate()
    if (duplicatedChild.eq(child)) {
      this
    } else {
      new ArraySerializer[T](duplicatedChild, clazz)
    }
  }

  override def getLength: Int = -1

  override def deserialize(source: DataInputView): Array[T] = {
    val length = source.readInt()
    val array  = new Array[T](length)
    var i      = 0
    while (i < length) {
      array(i) = child.deserialize(source)
      i += 1
    }
    array
  }

  override def serialize(record: Array[T], target: DataOutputView): Unit = {
    val length = record.length
    target.writeInt(length)
    var i = 0
    while (i < length) { // while loop is significantly faster than foreach when working on arrays
      val element = record(i)
      child.serialize(element, target)
      i += 1
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[Array[T]] =
    new CollectionSerializerSnapshot[Array, T, ArraySerializer[T]](child, classOf[ArraySerializer[T]], clazz)

}
