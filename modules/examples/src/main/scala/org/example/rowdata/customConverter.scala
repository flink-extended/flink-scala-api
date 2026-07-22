package org.example.rowdata

import org.apache.flink.table.data.RowData
import org.apache.flinkx.api.rowdata.{FieldConverter, RowDataConverter}

/** Customising how a single field is converted.
  *
  * `FieldConverter` instances are resolved '''by field type'''. So to give one field special treatment, give it a type
  * of its own — an `opaque type` costs nothing at runtime — and provide a `given FieldConverter` for that type.
  *
  * The alternative, a `given FieldConverter[Long]`, would silently apply to '''every''' `Long` field in every case
  * class in scope, which is almost never what you want.
  *
  * Note where `EpochSeconds` is declared: it has to be outside the scope that declares `Event`. Inside its own defining
  * scope an `opaque type` is transparent, so the derivation would see a plain `Long` there and quietly pick the
  * built-in converter instead of this one.
  */
object Time:

  /** A timestamp stored in the table as epoch '''milliseconds''', but exposed to the job as epoch '''seconds'''. */
  opaque type EpochSeconds = Long

  object EpochSeconds:
    def apply(seconds: Long): EpochSeconds = seconds

    extension (value: EpochSeconds) def seconds: Long = value

    given FieldConverter[EpochSeconds] with
      def fromRowData(row: RowData, index: Int): EpochSeconds = row.getLong(index) / 1000
      def toRowData(value: EpochSeconds): AnyRef              = java.lang.Long.valueOf(value * 1000)

import Time.EpochSeconds

// `userId` and `eventType` use the built-in String converter; only `ts` uses the custom one.
case class Event(userId: String, ts: EpochSeconds, eventType: String) derives RowDataConverter

@main def customConverterExample =
  val converter = summon[RowDataConverter[Event]]

  val event = Event("u1", EpochSeconds(1700000000L), "click")
  val row   = converter.toRowData(event)

  // Written back out in milliseconds, as the table's schema expects.
  println(s"stored millis: ${row.getLong(1)}") // 1700000000000

  // And read back in seconds.
  println(s"read back: ${converter.fromRowData(row)}") // Event(u1,1700000000,click)
