# Converting `RowData` to case classes

**Scala 3 only.** `RowData` is Flink's internal row format: it is what table connectors and formats hand you, and unlike
`Row` it has no generic `getField` — only typed accessors such as `getInt`, `getLong` and `getString`. Writing those
calls by hand is tedious and easy to get wrong. `RowDataConverter` derives them at compile time:

```scala
import org.apache.flinkx.api.rowdata.*

case class User(id: String, name: String, age: Int) derives RowDataConverter

val user: User    = row.toScala[User]
val back: RowData = user.toRowData
```

Fields are mapped **by position**: column `i` of the `RowData` corresponds to the `i`-th field in declaration order. Nothing
checks this against the table schema, so a case class whose fields are declared in a different order than the table's
columns will read garbage rather than fail. You must keep the case class and the schema in sync.

`toRowData` produces an `INSERT` row by default. To emit a changelog row of another kind — `UPDATE_BEFORE`,
`UPDATE_AFTER` or `DELETE` — pass the `RowKind` explicitly, for example forwarding the kind of the row you read:

```scala
val out: RowData = user.toRowData(sourceRow.getRowKind)
```

Derivation is available three ways, mirroring how `TypeInformation` is derived elsewhere in this library: the `derives`
clause above, `semiauto.deriveRowDataConverter[T]` for a converter you place in a companion object, and `auto.given` to
derive at every use site.

## Supported field types

Out of the box: all seven primitives, `String`, `Array[Byte]`, `Option` for nullable columns, and nested case classes
that have a converter of their own.

Wrapping a nullable column in `Option` matters. `RowData`'s typed accessors do not check nullity themselves, so a `NULL`
column read into a plain `Int` yields `0`, and into a plain `String` throws. The same applies to nested case classes: a
`NULL` ROW column read into a plain nested field throws — wrap it in `Option` to read a nullable ROW column.

`DECIMAL(p, s)` and `TIMESTAMP(p)` have no default given, because reading them at the wrong precision returns wrong
values rather than failing. State the schema explicitly:

```scala
given FieldConverter[BigDecimal] = FieldConverter.decimal(precision = 10, scale = 2)
given FieldConverter[Instant]    = FieldConverter.instant(precision = 3)
```

## Customising a single field

`FieldConverter` instances are resolved **by field type**. To give one field special treatment, give it a type of its
own — an `opaque type` costs nothing at runtime — and provide a `given` for that type:

```scala
object Time:
  opaque type EpochSeconds = Long

  object EpochSeconds:
    def apply(seconds: Long): EpochSeconds = seconds

    given FieldConverter[EpochSeconds] with
      def fromRowData(row: RowData, index: Int): EpochSeconds = row.getLong(index) / 1000
      def toRowData(value: EpochSeconds): AnyRef              = java.lang.Long.valueOf(value * 1000)

import Time.EpochSeconds

// only `ts` uses the custom converter; `userId` uses the built-in String one
case class Event(userId: String, ts: EpochSeconds) derives RowDataConverter
```

The alternative — a bare `given FieldConverter[Long]` — would silently apply to *every* `Long` field in scope, which is
rarely what you want.

Two gotchas worth knowing:

- **You must declare the `opaque type` outside the scope that declares the case class.** Within its own defining scope an
  `opaque type` is transparent, so a derivation there sees the underlying type (plain `Long` above) and quietly picks
  the built-in converter instead of yours. Putting the opaque type in its own object or file is enough.
- A `given` must be in scope where the converter is *derived*, not where it is used. Put it in the field type's
  companion object, or top-level in the file that declares the case class.

Runnable versions of all of the above are in
[modules/examples/src/main/scala/org/example/rowdata](https://github.com/flink-extended/flink-scala-api/tree/master/modules/examples/src/main/scala/org/example/rowdata).
