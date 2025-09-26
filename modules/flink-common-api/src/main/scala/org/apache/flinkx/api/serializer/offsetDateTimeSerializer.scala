package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer
import org.apache.flink.api.common.typeutils.{SimpleTypeSerializerSnapshot, TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import java.io.IOException
import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset}

/** Serializer for [[OffsetDateTime]]. Handle null value. */
object OffsetDateTimeSerializer extends ImmutableSerializer[OffsetDateTime] {

  private val localDateTimeSerializer: TypeSerializer[LocalDateTime] = LocalDateTimeSerializer.INSTANCE
  private val zoneOffsetSerializer: TypeSerializer[ZoneOffset]       = ZoneOffsetSerializer

  override def createInstance: OffsetDateTime = OffsetDateTime.now()

  override def getLength: Int = localDateTimeSerializer.getLength + zoneOffsetSerializer.getLength

  override def serialize(offsetDateTime: OffsetDateTime, target: DataOutputView): Unit =
    if (offsetDateTime == null) {
      localDateTimeSerializer.serialize(null, target)
      zoneOffsetSerializer.serialize(null, target)
    } else {
      localDateTimeSerializer.serialize(offsetDateTime.toLocalDateTime, target)
      zoneOffsetSerializer.serialize(offsetDateTime.getOffset, target)
    }

  override def deserialize(source: DataInputView): OffsetDateTime = {
    val localDateTime = localDateTimeSerializer.deserialize(source)
    val zoneOffset    = zoneOffsetSerializer.deserialize(source)
    if (localDateTime == null && zoneOffset == null) {
      null
    } else if (localDateTime == null || zoneOffset == null) {
      throw new IOException("LocalDateTime and ZoneOffset should be either both non-null, or both null")
    } else {
      OffsetDateTime.of(localDateTime, zoneOffset)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    localDateTimeSerializer.copy(source, target)
    zoneOffsetSerializer.copy(source, target)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[OffsetDateTime] =
    new OffsetDateTimeSerializerSnapshot()

}

/** Serializer snapshot for [[OffsetDateTime]]. */
class OffsetDateTimeSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[OffsetDateTime](() => OffsetDateTimeSerializer)
