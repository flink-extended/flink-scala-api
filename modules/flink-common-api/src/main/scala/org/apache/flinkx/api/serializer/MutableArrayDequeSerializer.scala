package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}

import scala.collection.mutable

/** Serializer for [[mutable.ArrayDeque]]. Handle nullable value. */
class MutableArrayDequeSerializer[A](child: TypeSerializer[A], clazz: Class[A])
    extends MutableSerializer[mutable.ArrayDeque[A]] {

  override def copy(from: mutable.ArrayDeque[A]): mutable.ArrayDeque[A] =
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

  override def duplicate(): MutableArrayDequeSerializer[A] = {
    val duplicatedChild = child.duplicate()
    if (duplicatedChild.eq(child)) {
      this
    } else {
      new MutableArrayDequeSerializer[A](duplicatedChild, clazz)
    }
  }

  override def createInstance(): mutable.ArrayDeque[A] = mutable.ArrayDeque.empty[A]

  override def getLength: Int = VariableLengthDataType

  override def serialize(records: mutable.ArrayDeque[A], target: DataOutputView): Unit =
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

  override def deserialize(source: DataInputView): mutable.ArrayDeque[A] = {
    var remaining = source.readInt()
    if (remaining == NullMarker) {
      null
    } else {
      val arrayDeque = createInstance()
      while (remaining > 0) {
        arrayDeque.append(child.deserialize(source))
        remaining -= 1
      }
      arrayDeque
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

  override def snapshotConfiguration(): TypeSerializerSnapshot[mutable.ArrayDeque[A]] =
    new CollectionSerializerSnapshot[mutable.ArrayDeque, A, MutableArrayDequeSerializer[A]](
      child,
      classOf[MutableArrayDequeSerializer[A]],
      clazz
    )

}
