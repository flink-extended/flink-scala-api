package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}

import java.util.Objects
import scala.collection.immutable.SortedSet
import scala.collection.mutable

/** Serializer for [[SortedSet]]. Handle nullable value. */
class SortedSetSerializer[A](
    val aSerializer: TypeSerializer[A],
    val aClass: Class[A],
    val aOrderingSerializer: TypeSerializer[Ordering[A]]
) extends MutableSerializer[SortedSet[A]] { // SortedSet is immutable, but its elements can be mutable

  override val isImmutableType: Boolean = aSerializer.isImmutableType && aOrderingSerializer.isImmutableType

  override def duplicate(): SortedSetSerializer[A] = {
    val duplicatedASerializer         = aSerializer.duplicate()
    val duplicatedAOrderingSerializer = aOrderingSerializer.duplicate()
    if (duplicatedASerializer.eq(aSerializer) && duplicatedAOrderingSerializer.eq(aOrderingSerializer)) {
      this
    } else {
      new SortedSetSerializer[A](duplicatedASerializer, aClass, duplicatedAOrderingSerializer)
    }
  }

  override def createInstance(): SortedSet[A] = SortedSet.empty[A](aOrderingSerializer.createInstance())

  override def copy(from: SortedSet[A]): SortedSet[A] =
    if (from == null || isImmutableType) {
      from
    } else {
      implicit val ordering: Ordering[A] =
        if (aOrderingSerializer.isImmutableType) from.ordering else aOrderingSerializer.copy(from.ordering)
      if (aSerializer.isImmutableType) {
        SortedSet.from(from)
      } else {
        from.map(aSerializer.copy)
      }
    }

  override def getLength: Int = VariableLengthDataType

  override def serialize(records: SortedSet[A], target: DataOutputView): Unit =
    if (records == null) {
      target.writeInt(NullMarker)
    } else {
      target.writeInt(records.size)
      records.foreach(element => aSerializer.serialize(element, target))
      aOrderingSerializer.serialize(records.ordering, target)
    }

  override def deserialize(source: DataInputView): SortedSet[A] = {
    var remaining = source.readInt()
    if (remaining == NullMarker) {
      null
    } else {
      val buffer = mutable.Buffer[A]()
      while (remaining > 0) {
        buffer.append(aSerializer.deserialize(source))
        remaining -= 1
      }
      implicit val ordering: Ordering[A] = aOrderingSerializer.deserialize(source)
      buffer.to(SortedSet)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    var remaining = source.readInt()
    target.writeInt(remaining)
    if (remaining >= 0) {
      while (remaining > 0) {
        aSerializer.copy(source, target)
        remaining -= 1
      }
      aOrderingSerializer.copy(source, target)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[SortedSet[A]] =
    new SortedCollectionSerializerSnapshot[SortedSet, A, SortedSetSerializer[A]](
      aSerializer,
      classOf[SortedSetSerializer[A]],
      aClass,
      aOrderingSerializer
    )

  override def hashCode(): Int = Objects.hash(aSerializer, aClass, aOrderingSerializer)

  override def equals(obj: Any): Boolean =
    obj match {
      case other: SortedSetSerializer[_] =>
        aSerializer == other.aSerializer && aClass == other.aClass && aOrderingSerializer == other.aOrderingSerializer
      case _ => false
    }

}
