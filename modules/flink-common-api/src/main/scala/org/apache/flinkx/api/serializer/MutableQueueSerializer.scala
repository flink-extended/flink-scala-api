package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}

import scala.collection.mutable

/** Serializer for [[mutable.Queue]]. Handle nullable value. */
class MutableQueueSerializer[A](child: TypeSerializer[A], clazz: Class[A]) extends MutableSerializer[mutable.Queue[A]] {

  override def copy(from: mutable.Queue[A]): mutable.Queue[A] =
    if (from == null) {
      from
    } else {
      val length = from.length
      val result = from.clone()
      if (!child.isImmutableType) {
        var i = 0
        while (i < length) {
          val element = result(i)
          if (element != null) result(i) = child.copy(element)
          i += 1
        }
      }
      result
    }

  override def duplicate(): MutableQueueSerializer[A] = {
    val duplicatedChild = child.duplicate()
    if (duplicatedChild.eq(child)) {
      this
    } else {
      new MutableQueueSerializer[A](duplicatedChild, clazz)
    }
  }

  override def createInstance(): mutable.Queue[A] = mutable.Queue.empty[A]

  override def getLength: Int = VariableLengthDataType

  override def serialize(records: mutable.Queue[A], target: DataOutputView): Unit =
    if (records == null) {
      target.writeInt(NullMarker)
    } else {
      target.writeInt(records.length)
      var i = 0
      while (i < records.length) { // while loop is significantly faster than foreach when working on arrays
        child.serialize(records(i), target)
        i += 1
      }
    }

  override def deserialize(source: DataInputView): mutable.Queue[A] = {
    var remaining = source.readInt()
    if (remaining == NullMarker) {
      null
    } else {
      val queue = createInstance()
      while (remaining > 0) {
        val a = child.deserialize(source)
        queue.append(a)
        remaining -= 1
      }
      queue
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    var remaining = source.readInt()
    target.writeInt(remaining)
    while (remaining > 0) {
      child.copy(source, target)
      remaining -= 1
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[mutable.Queue[A]] =
    new CollectionSerializerSnapshot[mutable.Queue, A, MutableQueueSerializer[A]](
      child,
      classOf[MutableQueueSerializer[A]],
      clazz
    )

}
