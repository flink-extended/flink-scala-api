package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{SimpleTypeSerializerSnapshot, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.NullMarker

import java.lang.Integer.{BYTES => IntBytes}
import java.time.ZoneOffset

/** Serializer for [[ZoneOffset]]. Handle null value. */
object ZoneOffsetSerializer extends ImmutableSerializer[ZoneOffset] {

  override def createInstance: ZoneOffset = ZoneOffset.UTC

  override def getLength: Int = IntBytes // 1 Int

  override def serialize(zoneOffset: ZoneOffset, target: DataOutputView): Unit =
    if (zoneOffset == null) {
      target.writeInt(NullMarker)
    } else {
      target.writeInt(zoneOffset.getTotalSeconds)
    }

  override def deserialize(source: DataInputView): ZoneOffset = {
    val totalSeconds = source.readInt()
    if (totalSeconds == NullMarker) {
      null
    } else {
      ZoneOffset.ofTotalSeconds(totalSeconds)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    target.writeInt(source.readInt())
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[ZoneOffset] = new ZoneOffsetSerializerSnapshot

}

/** Serializer snapshot for [[ZoneOffset]]. */
class ZoneOffsetSerializerSnapshot extends SimpleTypeSerializerSnapshot[ZoneOffset](() => ZoneOffsetSerializer)
