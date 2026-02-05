package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{SimpleTypeSerializerSnapshot, TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.VariableLengthDataType
import org.apache.flinkx.api.semiauto.{infoToSer, timeUnitInfo}

import java.lang.Long.{BYTES => LongBytes}
import scala.concurrent.duration.{Duration, FiniteDuration, TimeUnit}

/** Serializer for [[Duration]]. Handle nullable value. */
object DurationSerializer extends ImmutableSerializer[Duration] {

  val NullDiscriminant                     = -1
  val FiniteDurationDiscriminant           = 0
  val UndefinedDurationDiscriminant        = 1
  val PositiveInfiniteDurationDiscriminant = 2
  val NegativeInfiniteDurationDiscriminant = 3

  override def createInstance: Duration = Duration.Zero

  override def getLength: Int = VariableLengthDataType

  override def serialize(duration: Duration, target: DataOutputView): Unit =
    duration match {
      case null => target.writeByte(NullDiscriminant) // Cannot use NullMarker here as we are writing a byte
      case finiteDuration: FiniteDuration =>
        target.writeByte(FiniteDurationDiscriminant)
        FiniteDurationSerializer.serialize(finiteDuration, target)
      case Duration.Inf         => target.writeByte(PositiveInfiniteDurationDiscriminant)
      case Duration.MinusInf    => target.writeByte(NegativeInfiniteDurationDiscriminant)
      case _: Duration.Infinite => // Last to handle Duration.Undefined which doesn't equal itself
        target.writeByte(UndefinedDurationDiscriminant)
    }

  override def deserialize(source: DataInputView): Duration =
    source.readByte() match {
      case NullDiscriminant                     => null
      case FiniteDurationDiscriminant           => FiniteDurationSerializer.deserialize(source)
      case UndefinedDurationDiscriminant        => Duration.Undefined
      case PositiveInfiniteDurationDiscriminant => Duration.Inf
      case NegativeInfiniteDurationDiscriminant => Duration.MinusInf
      case _                                    => throw new IllegalArgumentException("Unknown duration type")
    }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val discriminant = source.readByte()
    target.writeByte(discriminant)
    if (discriminant == FiniteDurationDiscriminant) FiniteDurationSerializer.copy(source, target)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[Duration] = new DurationSerializerSnapshot()

}

/** Serializer snapshot for [[Duration]]. */
class DurationSerializerSnapshot extends SimpleTypeSerializerSnapshot[Duration](() => DurationSerializer)

/** Serializer for [[FiniteDuration]].
  *
  * Don't handle null value. If you need a nullable finite duration, use [[DurationSerializer]].
  */
object FiniteDurationSerializer extends ImmutableSerializer[FiniteDuration] {

  private val timeUnitSerializer: TypeSerializer[TimeUnit] = infoToSer(timeUnitInfo)

  override def createInstance: FiniteDuration = Duration.Zero

  override def getLength: Int = LongBytes + timeUnitSerializer.getLength // 1 Long + TimeUnit

  override def serialize(finiteDuration: FiniteDuration, target: DataOutputView): Unit = {
    target.writeLong(finiteDuration.length)
    timeUnitSerializer.serialize(finiteDuration.unit, target)
  }

  override def deserialize(source: DataInputView): FiniteDuration = {
    val length = source.readLong()
    val unit   = timeUnitSerializer.deserialize(source)
    new FiniteDuration(length, unit)
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    target.writeLong(source.readLong())
    timeUnitSerializer.copy(source, target)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[FiniteDuration] = new FiniteDurationSerializerSnapshot()

}

/** Serializer snapshot for [[FiniteDuration]]. */
class FiniteDurationSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[FiniteDuration](() => FiniteDurationSerializer)
