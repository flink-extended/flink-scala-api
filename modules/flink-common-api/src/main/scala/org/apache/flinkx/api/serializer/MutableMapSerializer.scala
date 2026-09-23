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
    new MutableMapSerializerSnapshot(keySerializer.snapshotConfiguration(), valueSerializer.snapshotConfiguration())

}

class MutableMapSerializerSnapshot[K, V](
    private var keySnapshot: TypeSerializerSnapshot[K],
    private var valueSnapshot: TypeSerializerSnapshot[V]
) extends TypeSerializerSnapshot[mutable.Map[K, V]] {

  def this() = this(null, null)

  override def getCurrentVersion: Int = 1

  override def writeSnapshot(out: DataOutputView): Unit = {
    TypeSerializerSnapshot.writeVersionedSnapshot(out, keySnapshot)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, valueSnapshot)
  }

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    keySnapshot = TypeSerializerSnapshot.readVersionedSnapshot[K](in, userCodeClassLoader)
    valueSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[V](in, userCodeClassLoader)
  }

  /** Compatible when the serializers of the keys and of the values are compatible. */
  override def resolveSchemaCompatibility(
      restoredSnapshot: TypeSerializerSnapshot[mutable.Map[K, V]]
  ): TypeSerializerSchemaCompatibility[mutable.Map[K, V]] = restoredSnapshot match {
    case restored: MutableMapSerializerSnapshot[_, _] =>
      SerializerUtil.resolveNestedSchemaCompatibility(
        Array(keySnapshot, valueSnapshot),
        Array(restored.keySnapshot, restored.valueSnapshot),
        restoreSerializerWith
      )
    case _ =>
      TypeSerializerSchemaCompatibility.incompatible()
  }

  private def restoreSerializerWith(
      nestedSerializers: Array[TypeSerializer[_]]
  ): TypeSerializer[mutable.Map[K, V]] =
    new MutableMapSerializer(
      nestedSerializers(0).asInstanceOf[TypeSerializer[K]],
      nestedSerializers(1).asInstanceOf[TypeSerializer[V]]
    )

  override def restoreSerializer(): TypeSerializer[mutable.Map[K, V]] =
    restoreSerializerWith(Array(keySnapshot.restoreSerializer(), valueSnapshot.restoreSerializer()))

}
