package org.example.rowdata

import org.apache.flink.table.data.{DecimalData, GenericRowData, StringData}
import org.apache.flinkx.api.rowdata.*

/** Nested rows, nullable columns, and the types that need their schema stated explicitly.
  *
  * Three things worth knowing:
  *
  *   - A `NULL` column read into a plain `Int` yields `0`, and into a plain `String` throws, because `RowData`'s typed
  *     accessors do not check nullity. Wrap nullable columns in `Option` and you get `None` instead.
  *   - A nested case class maps to a nested `ROW` column, and needs its own `RowDataConverter`.
  *   - `DECIMAL(p, s)` and `TIMESTAMP(p)` carry precision in the schema, and reading them at the wrong precision
  *     returns wrong values rather than failing. There is deliberately no default given for them: use the
  *     `FieldConverter.decimal` / `FieldConverter.instant` factories and state the precision from your table.
  */

/** The `balance` column below is declared in the table as DECIMAL(10, 2).
  *
  * This has to be a top-level given, so that it is in lexical scope where `Customer` is derived. Tucking it inside an
  * `object` would leave it invisible to the derivation, which would then fail to find any converter for `BigDecimal`.
  */
given FieldConverter[BigDecimal] = FieldConverter.decimal(precision = 10, scale = 2)

case class Address(city: String, country: String) derives RowDataConverter

case class Customer(
    id: String,
    address: Address,         // nested ROW column
    nickname: Option[String], // nullable column
    balance: BigDecimal       // DECIMAL(10, 2), via the given above
) derives RowDataConverter

@main def nestedAndNullableExample =
  val converter = summon[RowDataConverter[Customer]]

  val withNickname = GenericRowData.of(
    StringData.fromString("c1"),
    GenericRowData.of(StringData.fromString("Berlin"), StringData.fromString("DE")),
    StringData.fromString("Ally"),
    DecimalData.fromBigDecimal(new java.math.BigDecimal("19.99"), 10, 2)
  )

  val withoutNickname = GenericRowData.of(
    StringData.fromString("c2"),
    GenericRowData.of(StringData.fromString("Paris"), StringData.fromString("FR")),
    null, // NULL column
    DecimalData.fromBigDecimal(new java.math.BigDecimal("0.00"), 10, 2)
  )

  println(converter.fromRowData(withNickname))
  println(converter.fromRowData(withoutNickname))

  // Round-tripping preserves the NULL.
  println(converter.toRowData(converter.fromRowData(withoutNickname)).isNullAt(2))
