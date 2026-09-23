package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}

import scala.collection.SortedMap

/** Serializer for [[SortedMap]]. Handle nullable value. */
class SortedMapSerializer[K, V](
    keySerializer: TypeSerializer[K],
    valueSerializer: TypeSerializer[V],
    kOrderingSerializer: TypeSerializer[Ordering[K]]
) extends MutableSerializer[SortedMap[K, V]] { // SortedMap is immutable, but its elements can be mutable

  override val isImmutableType: Boolean =
    keySerializer.isImmutableType && valueSerializer.isImmutableType && kOrderingSerializer.isImmutableType

  override def copy(from: SortedMap[K, V]): SortedMap[K, V] =
    if (from == null || isImmutableType) {
      from
    } else {
      implicit val ordering: Ordering[K] = kOrderingSerializer.copy(from.ordering)
      from.map(element => (keySerializer.copy(element._1), valueSerializer.copy(element._2)))
    }

  override def duplicate(): SortedMapSerializer[K, V] = {
    val duplicatedKs = keySerializer.duplicate()
    val duplicatedVs = valueSerializer.duplicate()
    val duplicatedOs = kOrderingSerializer.duplicate()
    if (duplicatedKs.eq(keySerializer) && duplicatedVs.eq(valueSerializer) && duplicatedOs.eq(kOrderingSerializer)) {
      this
    } else {
      new SortedMapSerializer(duplicatedKs, duplicatedVs, duplicatedOs)
    }
  }

  override def createInstance(): SortedMap[K, V] = SortedMap.empty[K, V](kOrderingSerializer.createInstance())

  override def getLength: Int = VariableLengthDataType

  override def serialize(records: SortedMap[K, V], target: DataOutputView): Unit =
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

  override def deserialize(source: DataInputView): SortedMap[K, V] = {
    var remaining = source.readInt() // The valid range of actual data is >= 0. Only markers are negative
    if (remaining == NullMarker) {
      null
    } else {
      implicit val ordering: Ordering[K] = kOrderingSerializer.deserialize(source)
      val builder                        = SortedMap.newBuilder[K, V]
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

  override def snapshotConfiguration(): TypeSerializerSnapshot[SortedMap[K, V]] =
    new SortedMapSerializerSnapshot(
      keySerializer.snapshotConfiguration(),
      valueSerializer.snapshotConfiguration(),
      kOrderingSerializer.snapshotConfiguration()
    )

}

class SortedMapSerializerSnapshot[K, V](
    private var keySnapshot: TypeSerializerSnapshot[K],
    private var valueSnapshot: TypeSerializerSnapshot[V],
    private var kOrderingSnapshot: TypeSerializerSnapshot[Ordering[K]]
) extends TypeSerializerSnapshot[SortedMap[K, V]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(null, null, null)

  override def getCurrentVersion: Int = 1

  override def writeSnapshot(out: DataOutputView): Unit = {
    TypeSerializerSnapshot.writeVersionedSnapshot(out, keySnapshot)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, valueSnapshot)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, kOrderingSnapshot)
  }

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    keySnapshot = TypeSerializerSnapshot.readVersionedSnapshot[K](in, userCodeClassLoader)
    valueSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[V](in, userCodeClassLoader)
    kOrderingSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[Ordering[K]](in, userCodeClassLoader)
  }

  /** Compatible when the serializers of the keys, of the values and of their ordering are compatible. */
  override def resolveSchemaCompatibility(
      restoredSnapshot: TypeSerializerSnapshot[SortedMap[K, V]]
  ): TypeSerializerSchemaCompatibility[SortedMap[K, V]] = restoredSnapshot match {
    case restored: SortedMapSerializerSnapshot[_, _] =>
      SerializerUtil.resolveNestedSchemaCompatibility(
        Array(keySnapshot, valueSnapshot, kOrderingSnapshot),
        Array(restored.keySnapshot, restored.valueSnapshot, restored.kOrderingSnapshot),
        restoreSerializerWith
      )
    case _ =>
      TypeSerializerSchemaCompatibility.incompatible()
  }

  private def restoreSerializerWith(nestedSerializers: Array[TypeSerializer[_]]): TypeSerializer[SortedMap[K, V]] =
    new SortedMapSerializer(
      nestedSerializers(0).asInstanceOf[TypeSerializer[K]],
      nestedSerializers(1).asInstanceOf[TypeSerializer[V]],
      nestedSerializers(2).asInstanceOf[TypeSerializer[Ordering[K]]]
    )

  override def restoreSerializer(): TypeSerializer[SortedMap[K, V]] = restoreSerializerWith(
    Array(keySnapshot.restoreSerializer(), valueSnapshot.restoreSerializer(), kOrderingSnapshot.restoreSerializer())
  )

}
