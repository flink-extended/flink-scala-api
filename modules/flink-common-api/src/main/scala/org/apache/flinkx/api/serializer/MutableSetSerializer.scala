package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}

import scala.collection.mutable

/** Serializer for [[mutable.Set]]. Handle nullable value. */
class MutableSetSerializer[A](child: TypeSerializer[A], clazz: Class[A]) extends MutableSerializer[mutable.Set[A]] {

  override def copy(from: mutable.Set[A]): mutable.Set[A] =
    if (from == null) {
      from
    } else if (child.isImmutableType) {
      from.clone()
    } else {
      from.map(child.copy)
    }

  override def duplicate(): MutableSetSerializer[A] = {
    val duplicatedChild = child.duplicate()
    if (duplicatedChild.eq(child)) {
      this
    } else {
      new MutableSetSerializer[A](duplicatedChild, clazz)
    }
  }

  override def createInstance(): mutable.Set[A] = mutable.Set.empty[A]

  override def getLength: Int = VariableLengthDataType

  override def serialize(records: mutable.Set[A], target: DataOutputView): Unit =
    if (records == null) {
      target.writeInt(NullMarker)
    } else {
      target.writeInt(records.size)
      records.foreach(element => child.serialize(element, target))
    }

  override def deserialize(source: DataInputView): mutable.Set[A] = {
    var remaining = source.readInt()
    if (remaining == NullMarker) {
      null
    } else {
      val set = createInstance()
      while (remaining > 0) {
        set.add(child.deserialize(source))
        remaining -= 1
      }
      set
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

  override def snapshotConfiguration(): TypeSerializerSnapshot[mutable.Set[A]] =
    new CollectionSerializerSnapshot[mutable.Set, A, MutableSetSerializer[A]](
      child,
      classOf[MutableSetSerializer[A]],
      clazz
    )

}
