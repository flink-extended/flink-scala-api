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

Out of the box: all seven primitives, `String`, `Array[Byte]`, `LocalDate` for `DATE` columns, `LocalTime` for `TIME` columns,
`Option` for nullable columns, and nested case classes that have a converter of their own.

`TIME` is stored as milliseconds since midnight whatever precision the schema declares, so a `LocalTime` carrying
microseconds or nanoseconds is truncated on write.

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
      def logicalType: LogicalType                            = new BigIntType(false)
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

## Getting a `TypeInformation[RowData]`

A `DataStream[RowData]` needs a `TypeInformation[RowData]`, and Flink derives none for you: `RowData` is an interface,
so the type information has to carry the row's schema. Every derived converter exposes that schema as a `RowType`:

```scala
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

given TypeInformation[RowData] = InternalTypeInfo.of(summon[RowDataConverter[User]].rowType)
```

Each field contributes one column, named after the field and typed by that field's `FieldConverter`. Columns are
`NOT NULL` unless the field is an `Option`, and a nested case class contributes a nested `ROW`. So `User` above yields:

```
ROW<`id` VARCHAR(2147483647) NOT NULL, `name` VARCHAR(2147483647) NOT NULL, `age` INT NOT NULL>
```

This makes the case class the source of truth for the schema, which is what you want when writing rows you own. When
**reading** an existing table, prefer the schema the table itself declares — a derived `RowType` cannot reproduce a
column type the Scala types do not show, such as a `CHAR(10)` or a `DECIMAL` precision other than the one the field's
converter states:

```scala
// Iceberg
val rowType = FlinkSchemaUtil.convert(icebergTable.schema())

// Table API / catalog
val rowType = tEnv.from("mytable").getResolvedSchema
  .toPhysicalRowDataType.getLogicalType.asInstanceOf[RowType]
```

A custom `FieldConverter` must declare its own `logicalType` — that is how its column shows up here. Declare it
`NOT NULL` (the `false` argument above); `Option` is what makes a column nullable, by copying the inner type.

Runnable versions of all of the above are in
[modules/examples/src/main/scala/org/example/rowdata](https://github.com/flink-extended/flink-scala-api/tree/master/modules/examples/src/main/scala/org/example/rowdata).
