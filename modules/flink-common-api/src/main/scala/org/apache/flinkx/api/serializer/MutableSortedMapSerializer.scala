package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}

import scala.collection.mutable

/** Serializer for [[mutable.SortedMap]]. Handle nullable value. */
class MutableSortedMapSerializer[K, V](
    keySerializer: TypeSerializer[K],
    valueSerializer: TypeSerializer[V],
    kOrderingSerializer: TypeSerializer[Ordering[K]]
) extends MutableSerializer[mutable.SortedMap[K, V]] {

  override def copy(from: mutable.SortedMap[K, V]): mutable.SortedMap[K, V] =
    if (from == null || isImmutableType) {
      from
    } else {
      implicit val ordering: Ordering[K] = kOrderingSerializer.copy(from.ordering)
      from.map(element => (keySerializer.copy(element._1), valueSerializer.copy(element._2)))
    }

  override def duplicate(): MutableSortedMapSerializer[K, V] = {
    val duplicatedKs = keySerializer.duplicate()
    val duplicatedVs = valueSerializer.duplicate()
    val duplicatedOs = kOrderingSerializer.duplicate()
    if (duplicatedKs.eq(keySerializer) && duplicatedVs.eq(valueSerializer) && duplicatedOs.eq(kOrderingSerializer)) {
      this
    } else {
      new MutableSortedMapSerializer(duplicatedKs, duplicatedVs, duplicatedOs)
    }
  }

  override def createInstance(): mutable.SortedMap[K, V] =
    mutable.SortedMap.empty[K, V](kOrderingSerializer.createInstance())

  override def getLength: Int = VariableLengthDataType

  override def serialize(records: mutable.SortedMap[K, V], target: DataOutputView): Unit =
    if (records == null) {
      target.writeInt(NullMarker)
    } else {
      target.writeInt(records.size)
      kOrderingSerializer.serialize(records.ordering, target)
      records.foreach(element => {
        keySerializer.serialize(element._1, target)
        valueSerializer.serialize(element._2, target)
      })
    }

  override def deserialize(source: DataInputView): mutable.SortedMap[K, V] = {
    var remaining = source.readInt() // The valid range of actual data is >= 0. Only markers are negative
    if (remaining == NullMarker) {
      null
    } else {
      implicit val ordering: Ordering[K] = kOrderingSerializer.deserialize(source)
      val builder                        = mutable.SortedMap.newBuilder[K, V]
      builder.sizeHint(remaining)
      while (remaining > 0) {
        val key   = keySerializer.deserialize(source)
        val value = valueSerializer.deserialize(source)
        builder.addOne(key -> value)
        remaining -= 1
      }
      builder.result()
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    var remaining = source.readInt()
    target.writeInt(remaining)
    if (remaining != NullMarker) {
      kOrderingSerializer.copy(source, target)
      while (remaining > 0) {
        keySerializer.copy(source, target)
        valueSerializer.copy(source, target)
        remaining -= 1
      }
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[mutable.SortedMap[K, V]] =
    new MutableSortedMapSerializerSnapshot(keySerializer, valueSerializer, kOrderingSerializer)

}

class MutableSortedMapSerializerSnapshot[K, V](
    private var keySerializer: TypeSerializer[K],
    private var valueSerializer: TypeSerializer[V],
    private var kOrderingSerializer: TypeSerializer[Ordering[K]]
) extends TypeSerializerSnapshot[mutable.SortedMap[K, V]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(null, null, null)

  override def getCurrentVersion: Int = 1

  override def writeSnapshot(out: DataOutputView): Unit = {
    TypeSerializerSnapshot.writeVersionedSnapshot(out, keySerializer.snapshotConfiguration())
    TypeSerializerSnapshot.writeVersionedSnapshot(out, valueSerializer.snapshotConfiguration())
    TypeSerializerSnapshot.writeVersionedSnapshot(out, kOrderingSerializer.snapshotConfiguration())
  }

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    keySerializer = TypeSerializerSnapshot.readVersionedSnapshot[K](in, userCodeClassLoader).restoreSerializer()
    valueSerializer = TypeSerializerSnapshot.readVersionedSnapshot[V](in, userCodeClassLoader).restoreSerializer()
    kOrderingSerializer = TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader).restoreSerializer()
  }

  override def resolveSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[mutable.SortedMap[K, V]]
  ): TypeSerializerSchemaCompatibility[mutable.SortedMap[K, V]] =
    TypeSerializerSchemaCompatibility.compatibleAsIs()

  override def restoreSerializer(): TypeSerializer[mutable.SortedMap[K, V]] =
    new MutableSortedMapSerializer(keySerializer, valueSerializer, kOrderingSerializer)

}
