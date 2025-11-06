package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}

import scala.collection.mutable

/** Serializer for [[mutable.Buffer]]. Handle nullable value. */
class MutableBufferSerializer[A](child: TypeSerializer[A], clazz: Class[A])
    extends MutableSerializer[mutable.Buffer[A]] {

  override def copy(from: mutable.Buffer[A]): mutable.Buffer[A] =
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

  override def duplicate(): MutableBufferSerializer[A] = {
    val duplicatedChild = child.duplicate()
    if (duplicatedChild.eq(child)) {
      this
    } else {
      new MutableBufferSerializer[A](duplicatedChild, clazz)
    }
  }

  override def createInstance(): mutable.Buffer[A] = mutable.Buffer.empty[A]

  override def getLength: Int = VariableLengthDataType

  override def serialize(records: mutable.Buffer[A], target: DataOutputView): Unit =
    if (records == null) {
      target.writeInt(NullMarker)
    } else {
      target.writeInt(records.length)
      records match {
        case _: mutable.ArrayBuffer[_] | _: mutable.ArrayDeque[_] =>
          var i = 0
          while (i < records.length) { // while loop is significantly faster than foreach when working on arrays
            child.serialize(records(i), target)
            i += 1
          }
        case _ => records.foreach(element => child.serialize(element, target))
      }
    }

  override def deserialize(source: DataInputView): mutable.Buffer[A] = {
    var remaining = source.readInt()
    if (remaining == NullMarker) {
      null
    } else {
      val buffer = createInstance()
      while (remaining > 0) {
        buffer.append(child.deserialize(source))
        remaining -= 1
      }
      buffer
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

  override def snapshotConfiguration(): TypeSerializerSnapshot[mutable.Buffer[A]] =
    new CollectionSerializerSnapshot[mutable.Buffer, A, MutableBufferSerializer[A]](
      child,
      classOf[MutableBufferSerializer[A]],
      clazz
    )

}
