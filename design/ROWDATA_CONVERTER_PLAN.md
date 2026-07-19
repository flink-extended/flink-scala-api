# RowData ↔ Case Class Converter — Design Notes

Status: **implemented**. This document records what was built and, where it diverges, why. It replaces an earlier
forward-looking plan whose central assumptions did not survive contact with the Flink API — those divergences are
called out below so the reasoning is not lost.

## What it does

Converts between Flink's internal `org.apache.flink.table.data.RowData` format and Scala 3 case classes, deriving the
conversion at compile time:

```scala
import org.apache.flinkx.api.rowdata.*

case class User(id: String, name: String, age: Int) derives RowDataConverter

val user: User    = row.toScala[User]
val back: RowData = user.toRowData
```

Fields map **by position**: column `i` of the `RowData` is the `i`-th field in declaration order. This is unchecked
against any table schema — a mismatched declaration order reads wrong data rather than failing.

## Files

Main sources — `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/`:

| File | Role |
|------|------|
| `FieldConverter.scala` | Per-field conversion trait + built-in givens (primitives, `String`, `Array[Byte]`, `Option`, nested case class) + `decimal`/`instant`/`localDateTime` factories |
| `RowDataConverter.scala` | Public trait (`fromRowData`, `toRowData`, `arity`) + `inline def derived` supporting `derives` |
| `auto.scala` / `semiauto.scala` | Automatic (`given`) and explicit (`deriveRowDataConverter`) derivation, mirroring the `TypeInformation` convention |
| `extensions.scala` | `row.toScala[T]` and `value.toRowData` syntax |

Tests: `modules/flink-common-api/src/test/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterTest.scala` (17 cases,
run against both Flink 1.20 and 2.0).

Examples: `modules/examples/src/main/scala/org/example/rowdata/` — `basicRowData` (full DataStream job),
`customConverter` (opaque-type override), `nestedAndNullable` (nested ROW, `Option`, `DECIMAL`).

## Divergences from the original plan

The original plan was written against an assumed API and needed three substantive corrections.

### 1. No generic field access — hence per-type givens, not `DefaultFieldConverter`

`RowData` has **no `getField(index)`**. It exposes only typed accessors (`getInt`, `getLong`, `getString: StringData`,
`getRow(pos, arity)`, `getDecimal(pos, precision, scale)`, …). The planned `DefaultFieldConverter[T](typeInfo)` — a
single converter parameterised by a `TypeInformation[T]` — was therefore unbuildable: there is no type-agnostic way to
read a column.

Instead each supported type has its own `given FieldConverter`, and the derivation summons one per field by type. This
also decided the customisation model (below).

### 2. Custom converters are resolved by type, not by field name

The plan's headline feature — `implicit val timestampConverter: FieldConverter[Long]` in the companion to customise the
`timestamp` field — does not work: it would apply to **every** `Long` field in scope, and the enum example's
`FieldConverter[String]` would have hijacked `orderId` alongside `status`.

Resolution is **by field type**. To customise one field, give it a distinct type (an `opaque type` costs nothing at
runtime) and provide a `given` for that type. Documented gotcha: the opaque type must be declared *outside* the scope
that declares the case class, because within its own defining scope it is transparent and the derivation sees the
underlying type. Three failing tests caught this during implementation.

### 3. `typeInfo` dropped from the trait

The planned `typeInfo: TypeInformation[T]` returning a `RowTypeInfo` was wrong twice over: `RowTypeInfo` describes
`org.apache.flink.types.Row`, not `RowData`, and a correct `InternalTypeInfo` of a `RowType` would have pulled a
`flink-table-runtime` dependency into `flink-common-api` plus a full Scala-type-to-`LogicalType` mapping. Conversion and
type-info are separate concerns and the project already derives `TypeInformation[T]` via Magnolia, so the trait was left
minimal. The examples show how to supply `InternalTypeInfo` at the use site when a `DataStream[RowData]` needs it.

## Implementation choice: inline, not a quoted macro

The plan called for a quoted macro emitting unrolled per-field code. The implementation is a plain `inline def` that
resolves one `FieldConverter` per field into an `Array` and iterates it in a `while` loop. Trade-off: one array load and
one monomorphic virtual call per field (both JIT-friendly) in exchange for far simpler, more maintainable code and no
`scala.quoted` surface. Still zero reflection. The trait boundary is unchanged, so swapping in a quoted macro later — if
a benchmark ever justifies it — is a contained change.

## Not done

Performance benchmarks and Table-API integration tests. Nothing currently exercises the converter through a running
Table pipeline end-to-end; the `basicRowData` example is the closest, driving a `DataStream[RowData]`.
