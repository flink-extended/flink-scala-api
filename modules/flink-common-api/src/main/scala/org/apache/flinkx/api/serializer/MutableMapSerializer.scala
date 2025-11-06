package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}

import scala.collection.mutable

/** Serializer for [[mutable.Map]]. Handle nullable value. */
class MutableMapSerializer[K, V](
    keySerializer: TypeSerializer[K],
    valueSerializer: TypeSerializer[V]
) extends MutableSerializer[mutable.Map[K, V]] {

  override def copy(from: mutable.Map[K, V]): mutable.Map[K, V] =
    if (from == null) {
      from
    } else {
      from.map(element => (keySerializer.copy(element._1), valueSerializer.copy(element._2)))
    }

  override def duplicate(): MutableMapSerializer[K, V] = {
    val duplicatedKs = keySerializer.duplicate()
    val duplicatedVs = valueSerializer.duplicate()
    if (duplicatedKs.eq(keySerializer) && duplicatedVs.eq(valueSerializer)) {
      this
    } else {
      new MutableMapSerializer(duplicatedKs, duplicatedVs)
    }
  }

  override def createInstance(): mutable.Map[K, V] = mutable.Map.empty[K, V]

  override def getLength: Int = VariableLengthDataType

  override def serialize(records: mutable.Map[K, V], target: DataOutputView): Unit =
    if (records == null) {
      target.writeInt(NullMarker)
    } else {
      target.writeInt(records.size)
      records.foreach(element => {
        keySerializer.serialize(element._1, target)
        valueSerializer.serialize(element._2, target)
      })
    }

  override def deserialize(source: DataInputView): mutable.Map[K, V] = {
    var remaining = source.readInt() // The valid range of actual data is >= 0. Only markers are negative
    if (remaining == NullMarker) {
      null
    } else {
      val map = createInstance()
      while (remaining > 0) {
        val key   = keySerializer.deserialize(source)
        val value = valueSerializer.deserialize(source)
        map.put(key, value)
        remaining -= 1
      }
      map
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    var remaining = source.readInt()
    target.writeInt(remaining)
    while (remaining > 0) {
      keySerializer.copy(source, target)
      valueSerializer.copy(source, target)
      remaining -= 1
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[mutable.Map[K, V]] =
    new MutableMapSerializerSnapshot(keySerializer, valueSerializer)

}

class MutableMapSerializerSnapshot[K, V](
    private var keySerializer: TypeSerializer[K],
    private var valueSerializer: TypeSerializer[V]
) extends TypeSerializerSnapshot[mutable.Map[K, V]] {

  def this() = this(null, null)

  override def getCurrentVersion: Int = 1

  override def writeSnapshot(out: DataOutputView): Unit = {
    TypeSerializerSnapshot.writeVersionedSnapshot(out, keySerializer.snapshotConfiguration())
    TypeSerializerSnapshot.writeVersionedSnapshot(out, valueSerializer.snapshotConfiguration())
  }

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    keySerializer = TypeSerializerSnapshot.readVersionedSnapshot[K](in, userCodeClassLoader).restoreSerializer()
    valueSerializer = TypeSerializerSnapshot.readVersionedSnapshot[V](in, userCodeClassLoader).restoreSerializer()
  }

  override def resolveSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[mutable.Map[K, V]]
  ): TypeSerializerSchemaCompatibility[mutable.Map[K, V]] = {
    TypeSerializerSchemaCompatibility.compatibleAsIs()
  }

  override def restoreSerializer(): TypeSerializer[mutable.Map[K, V]] =
    new MutableMapSerializer(keySerializer, valueSerializer)

}
