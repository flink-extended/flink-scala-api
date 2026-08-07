package org.apache.flinkx.api.rowdata

import org.apache.flink.table.data.{DecimalData, RowData, StringData, TimestampData}
import org.apache.flink.table.types.logical.*

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

/** Converts a single [[RowData]] column, at a known position, to and from a Scala field type.
  *
  * Instances are resolved '''by field type''' when a [[RowDataConverter]] is derived. To give one field of a case class
  * special treatment, give that field a distinct type (an `opaque type` or a wrapper) and provide a `given
  * FieldConverter` for it:
  *
  * {{{
  * opaque type EpochSeconds = Long
  *
  * object EpochSeconds:
  *   given FieldConverter[EpochSeconds] with
  *     def logicalType: LogicalType                            = new BigIntType(false)
  *     def fromRowData(row: RowData, index: Int): EpochSeconds = row.getLong(index) / 1000
  *     def toRowData(value: EpochSeconds): AnyRef              = java.lang.Long.valueOf(value * 1000)
  *
  * case class Event(userId: String, ts: EpochSeconds) derives RowDataConverter
  * }}}
  *
  * A `given` in the field type's companion object, or in lexical scope at the derivation site, takes precedence over
  * the built-in instances below, which live in this trait's companion and therefore have the lowest priority.
  *
  * '''Declare the `opaque type` outside the scope that declares the case class.''' Within its own defining scope an
  * `opaque type` is transparent, so a derivation there sees the underlying type — plain `Long` above — and silently
  * picks the built-in converter instead of the custom one. Putting the type in its own object, or its own file, avoids
  * this.
  *
  * @tparam A
  *   the Scala field type
  */
trait FieldConverter[A] extends Serializable {

  /** The Flink column type this converter reads and writes.
    *
    * Used to assemble [[RowDataConverter.rowType]], and through it a `TypeInformation[RowData]`. Declare it '''NOT
    * NULL''' (pass `false` as the `isNullable` argument of the [[LogicalType]] constructor) unless the field type is
    * itself nullable: [[optionConverter]] is what marks a column nullable, by copying the inner type.
    */
  def logicalType: LogicalType

  /** Reads column `index` of `row` and converts it to `A`.
    *
    * The index is passed explicitly (rather than being baked into the instance) so that a single instance can be shared
    * across positions and across records.
    */
  def fromRowData(row: RowData, index: Int): A

  /** Converts `value` to the internal representation Flink expects for this column, as accepted by
    * [[GenericRowData.setField]].
    *
    * No index is passed: writing a column is position-independent, the caller decides where the result lands.
    */
  def toRowData(value: A): AnyRef

}

/** Built-in [[FieldConverter]] instances for the types [[RowData]] can represent directly.
  *
  * These are the lowest-priority givens, so any instance a user puts in lexical scope or in a field type's companion
  * object wins over them.
  *
  * Two Flink types carry their own parameters and therefore have no sensible default: `DECIMAL(p, s)` and
  * `TIMESTAMP(p)`. Reading them requires the precision and scale declared in the table schema, and reading with the
  * wrong precision yields wrong values rather than an error. They are exposed as the explicit factories [[decimal]],
  * [[instant]] and [[localDateTime]] instead of givens, so the schema has to be stated at the use site.
  */
object FieldConverter {

  given booleanConverter: FieldConverter[Boolean] with {
    def logicalType: LogicalType                       = new BooleanType(false)
    def fromRowData(row: RowData, index: Int): Boolean = row.getBoolean(index)
    def toRowData(value: Boolean): AnyRef              = java.lang.Boolean.valueOf(value)
  }

  given byteConverter: FieldConverter[Byte] with {
    def logicalType: LogicalType                    = new TinyIntType(false)
    def fromRowData(row: RowData, index: Int): Byte = row.getByte(index)
    def toRowData(value: Byte): AnyRef              = java.lang.Byte.valueOf(value)
  }

  given shortConverter: FieldConverter[Short] with {
    def logicalType: LogicalType                     = new SmallIntType(false)
    def fromRowData(row: RowData, index: Int): Short = row.getShort(index)
    def toRowData(value: Short): AnyRef              = java.lang.Short.valueOf(value)
  }

  given intConverter: FieldConverter[Int] with {
    def logicalType: LogicalType                   = new IntType(false)
    def fromRowData(row: RowData, index: Int): Int = row.getInt(index)
    def toRowData(value: Int): AnyRef              = java.lang.Integer.valueOf(value)
  }

  given longConverter: FieldConverter[Long] with {
    def logicalType: LogicalType                    = new BigIntType(false)
    def fromRowData(row: RowData, index: Int): Long = row.getLong(index)
    def toRowData(value: Long): AnyRef              = java.lang.Long.valueOf(value)
  }

  given floatConverter: FieldConverter[Float] with {
    def logicalType: LogicalType                     = new FloatType(false)
    def fromRowData(row: RowData, index: Int): Float = row.getFloat(index)
    def toRowData(value: Float): AnyRef              = java.lang.Float.valueOf(value)
  }

  given doubleConverter: FieldConverter[Double] with {
    def logicalType: LogicalType                      = new DoubleType(false)
    def fromRowData(row: RowData, index: Int): Double = row.getDouble(index)
    def toRowData(value: Double): AnyRef              = java.lang.Double.valueOf(value)
  }

  given stringConverter: FieldConverter[String] with {
    def logicalType: LogicalType                      = new VarCharType(false, VarCharType.MAX_LENGTH)
    def fromRowData(row: RowData, index: Int): String = row.getString(index).toString
    def toRowData(value: String): AnyRef              = StringData.fromString(value)
  }

  /** A `DATE` column, which [[RowData]] stores as the number of days since the epoch in an `int`. */
  given localDateConverter: FieldConverter[LocalDate] with {
    def logicalType: LogicalType                         = new DateType(false)
    def fromRowData(row: RowData, index: Int): LocalDate = LocalDate.ofEpochDay(row.getInt(index).toLong)
    def toRowData(value: LocalDate): AnyRef              = java.lang.Integer.valueOf(value.toEpochDay.toInt)
  }

  /** A `TIME` column, which [[RowData]] stores as the number of milliseconds since midnight in an `int`.
    *
    * Flink's internal representation is milliseconds whatever precision the schema declares, so a [[LocalTime]]
    * carrying microseconds or nanoseconds is truncated on write.
    */
  given localTimeConverter: FieldConverter[LocalTime] with {
    def logicalType: LogicalType = new TimeType(false, 3)

    def fromRowData(row: RowData, index: Int): LocalTime =
      LocalTime.ofNanoOfDay(row.getInt(index).toLong * 1000000L)

    def toRowData(value: LocalTime): AnyRef = java.lang.Integer.valueOf((value.toNanoOfDay / 1000000L).toInt)
  }

  given binaryConverter: FieldConverter[Array[Byte]] with {
    def logicalType: LogicalType                           = new VarBinaryType(false, VarBinaryType.MAX_LENGTH)
    def fromRowData(row: RowData, index: Int): Array[Byte] = row.getBinary(index)
    def toRowData(value: Array[Byte]): AnyRef              = value
  }

  /** Makes any field nullable: a `NULL` column reads back as `None`, and `None` writes back as `NULL`.
    *
    * Without this wrapper a `NULL` column read as a primitive silently yields `0` (or an NPE for `String`), because
    * [[RowData]]'s typed accessors do not check nullity themselves.
    */
  given optionConverter[A](using inner: FieldConverter[A]): FieldConverter[Option[A]] with {
    def logicalType: LogicalType = inner.logicalType.copy(true)

    def fromRowData(row: RowData, index: Int): Option[A] =
      if row.isNullAt(index) then None else Some(inner.fromRowData(row, index))

    def toRowData(value: Option[A]): AnyRef = value match {
      case Some(a) => inner.toRowData(a)
      case None    => null
    }
  }

  /** Reads a nested case class from a `ROW` column, and writes it back as a nested [[GenericRowData]].
    *
    * The nested type's own [[RowDataConverter]] supplies the arity that [[RowData.getRow]] requires.
    */
  given nestedConverter[A](using nested: RowDataConverter[A]): FieldConverter[A] with {
    def logicalType: LogicalType = nested.rowType

    def fromRowData(row: RowData, index: Int): A =
      if row.isNullAt(index) then
        throw new NullPointerException(
          s"nested ROW column at index $index is NULL; wrap the field in Option to read a nullable ROW column"
        )
      else nested.fromRowData(row.getRow(index, nested.arity))

    def toRowData(value: A): AnyRef = nested.toRowData(value)
  }

  /** A converter for a `DECIMAL(precision, scale)` column. Both must match the table schema. */
  def decimal(precision: Int, scale: Int): FieldConverter[BigDecimal] =
    new FieldConverter[BigDecimal] {
      def logicalType: LogicalType = new DecimalType(false, precision, scale)

      def fromRowData(row: RowData, index: Int): BigDecimal =
        BigDecimal(row.getDecimal(index, precision, scale).toBigDecimal)

      def toRowData(value: BigDecimal): AnyRef = {
        val decimal = DecimalData.fromBigDecimal(value.bigDecimal, precision, scale)
        if decimal == null then
          throw new IllegalArgumentException(
            s"BigDecimal $value does not fit DECIMAL($precision, $scale)"
          )
        decimal
      }
    }

  /** A converter for a `TIMESTAMP_LTZ(precision)` column read as an [[Instant]]. */
  def instant(precision: Int): FieldConverter[Instant] =
    new FieldConverter[Instant] {
      def logicalType: LogicalType                       = new LocalZonedTimestampType(false, precision)
      def fromRowData(row: RowData, index: Int): Instant = row.getTimestamp(index, precision).toInstant
      def toRowData(value: Instant): AnyRef              = TimestampData.fromInstant(value)
    }

  /** A converter for a `TIMESTAMP(precision)` column read as a [[LocalDateTime]]. */
  def localDateTime(precision: Int): FieldConverter[LocalDateTime] =
    new FieldConverter[LocalDateTime] {
      def logicalType: LogicalType = new TimestampType(false, precision)

      def fromRowData(row: RowData, index: Int): LocalDateTime =
        row.getTimestamp(index, precision).toLocalDateTime

      def toRowData(value: LocalDateTime): AnyRef = TimestampData.fromLocalDateTime(value)
    }

}
