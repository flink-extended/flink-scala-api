package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer
import org.apache.flink.api.common.typeutils.{SimpleTypeSerializerSnapshot, TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.VariableLengthDataType

import java.io.IOException
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

/** Serializer for [[ZonedDateTime]]. Handle null value. */
object ZonedDateTimeSerializer extends ImmutableSerializer[ZonedDateTime] {

  private val localDateTimeSerializer: TypeSerializer[LocalDateTime] = LocalDateTimeSerializer.INSTANCE
  private val zoneIdSerializer: TypeSerializer[ZoneId]               = ZoneIdSerializer

  override def createInstance: ZonedDateTime = ZonedDateTime.now()

  override def getLength: Int = VariableLengthDataType

  override def serialize(zonedDateTime: ZonedDateTime, target: DataOutputView): Unit =
    if (zonedDateTime == null) {
      localDateTimeSerializer.serialize(null, target)
      zoneIdSerializer.serialize(null, target)
    } else {
      localDateTimeSerializer.serialize(zonedDateTime.toLocalDateTime, target)
      zoneIdSerializer.serialize(zonedDateTime.getZone, target)
    }

  override def deserialize(source: DataInputView): ZonedDateTime = {
    val localDateTime = localDateTimeSerializer.deserialize(source)
    val zoneId        = zoneIdSerializer.deserialize(source)
    if (localDateTime == null && zoneId == null) {
      null
    } else if (localDateTime == null || zoneId == null) {
      throw new IOException("LocalDateTime and ZoneId should be either both non-null, or both null")
    } else {
      ZonedDateTime.of(localDateTime, zoneId)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    localDateTimeSerializer.copy(source, target)
    zoneIdSerializer.copy(source, target)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[ZonedDateTime] =
    new ZonedDateTimeSerializerSnapshot()

}

/** Serializer snapshot for [[ZonedDateTime]]. */
class ZonedDateTimeSerializerSnapshot extends SimpleTypeSerializerSnapshot[ZonedDateTime](() => ZonedDateTimeSerializer)
