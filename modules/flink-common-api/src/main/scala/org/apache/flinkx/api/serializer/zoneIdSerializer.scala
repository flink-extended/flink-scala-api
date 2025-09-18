package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{SimpleTypeSerializerSnapshot, TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.VariableLengthDataType

import java.time.ZoneId

/** Serializer for [[ZoneId]]. Handle null value. */
object ZoneIdSerializer extends ImmutableSerializer[ZoneId] {

  private val stringSerializer: TypeSerializer[String] = org.apache.flinkx.api.serializers.stringSerializer

  override def createInstance: ZoneId = ZoneId.systemDefault()

  override def getLength: Int = VariableLengthDataType

  override def serialize(zoneId: ZoneId, target: DataOutputView): Unit =
    if (zoneId == null) {
      stringSerializer.serialize(null, target)
    } else {
      stringSerializer.serialize(zoneId.getId, target)
    }

  override def deserialize(source: DataInputView): ZoneId = {
    val id = stringSerializer.deserialize(source)
    if (id == null) {
      null
    } else {
      ZoneId.of(id)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit =
    stringSerializer.copy(source, target)

  override def snapshotConfiguration(): TypeSerializerSnapshot[ZoneId] =
    new ZoneIdSerializerSnapshot()

}

/** Serializer snapshot for [[ZoneId]]. */
class ZoneIdSerializerSnapshot extends SimpleTypeSerializerSnapshot[ZoneId](() => ZoneIdSerializer)
