package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}

import java.util.Objects
import scala.collection.mutable

/** Serializer for [[mutable.SortedSet]]. Handle nullable value. */
class MutableSortedSetSerializer[A](
    val aSerializer: TypeSerializer[A],
    val aClass: Class[A],
    val aOrderingSerializer: TypeSerializer[Ordering[A]]
) extends MutableSerializer[mutable.SortedSet[A]] {

  override def duplicate(): MutableSortedSetSerializer[A] = {
    val duplicatedASerializer         = aSerializer.duplicate()
    val duplicatedAOrderingSerializer = aOrderingSerializer.duplicate()
    if (duplicatedASerializer.eq(aSerializer) && duplicatedAOrderingSerializer.eq(aOrderingSerializer)) {
      this
    } else {
      new MutableSortedSetSerializer[A](duplicatedASerializer, aClass, duplicatedAOrderingSerializer)
    }
  }

  override def createInstance(): mutable.SortedSet[A] = mutable.SortedSet.empty[A](aOrderingSerializer.createInstance())

  override def copy(from: mutable.SortedSet[A]): mutable.SortedSet[A] =
    if (from == null || isImmutableType) {
      from
    } else {
      implicit val ordering: Ordering[A] = aOrderingSerializer.copy(from.ordering)
      from.map(aSerializer.copy)
    }

  override def getLength: Int = VariableLengthDataType

  override def serialize(records: mutable.SortedSet[A], target: DataOutputView): Unit =
    if (records == null) {
      target.writeInt(NullMarker)
    } else {
      target.writeInt(records.size)
      aOrderingSerializer.serialize(records.ordering, target)
      records.foreach(element => aSerializer.serialize(element, target))
    }

  override def deserialize(source: DataInputView): mutable.SortedSet[A] = {
    var remaining = source.readInt()
    if (remaining == NullMarker) {
      null
    } else {
      implicit val ordering: Ordering[A] = aOrderingSerializer.deserialize(source)
      val builder                        = mutable.SortedSet.newBuilder
      builder.sizeHint(remaining)
      while (remaining > 0) {
        builder.addOne(aSerializer.deserialize(source))
        remaining -= 1
      }
      builder.result()
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

  override def snapshotConfiguration(): TypeSerializerSnapshot[mutable.SortedSet[A]] =
    new SortedCollectionSerializerSnapshot[mutable.SortedSet, A, MutableSortedSetSerializer[A]](
      aSerializer,
      classOf[MutableSortedSetSerializer[A]],
      aClass,
      aOrderingSerializer
    )

  override def hashCode(): Int = Objects.hash(aSerializer, aClass, aOrderingSerializer)

  override def equals(obj: Any): Boolean =
    obj match {
      case other: MutableSortedSetSerializer[_] =>
        aSerializer == other.aSerializer && aClass == other.aClass && aOrderingSerializer == other.aOrderingSerializer
      case _ => false
    }

}
