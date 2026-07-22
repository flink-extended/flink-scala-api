# Detailed Design Reference: RowData to Scala Case Class Converter

This document contains complete code examples and detailed specifications referenced in ROWDATA_CONVERTER_PLAN.md.

## Code Examples

### Example 1: Basic Usage

```scala
import org.apache.flinkx.api.rowdata.auto._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.data.GenericRowData

case class User(id: String, name: String, age: Int) derives RowDataConverter

object BasicExample:
  def main(args: Array[String]): Unit =
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    val rowDataStream = env.fromData(
      GenericRowData.of("user1", "Alice", 30),
      GenericRowData.of("user2", "Bob", 25)
    )
    
    val userStream = rowDataStream.map { rowData =>
      rowData.toScala[User]
    }
    
    userStream.print()
    env.execute()
```

### Example 2: Custom Field Converter

```scala
import org.apache.flinkx.api.rowdata.auto._
import org.apache.flinkx.api.rowdata.FieldConverter
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.RowData

case class Event(
  userId: String,
  timestamp: Long,  // milliseconds in RowData
  eventType: String
) derives RowDataConverter

object Event:
  // Custom converter: convert milliseconds to seconds
  implicit val timestampConverter: FieldConverter[Long] = new FieldConverter[Long]:
    override def fromRowData(rowData: RowData, index: Int): Long =
      if rowData.isNullAt(index) then 0L
      else rowData.getLong(index) / 1000  // ms to seconds
    
    override def toRowData(value: Long, index: Int): Any =
      value * 1000  // seconds to ms
    
    override def typeInfo: TypeInformation[Long] =
      TypeInformation.of(classOf[Long])
```

### Example 3: Enum-Like Conversion

```scala
import org.apache.flinkx.api.rowdata.auto._
import org.apache.flinkx.api.rowdata.FieldConverter
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.RowData

case class Order(
  orderId: String,
  status: String  // PENDING, CONFIRMED, SHIPPED
) derives RowDataConverter

object Order:
  // Convert between status codes (stored as Int in RowData) and strings
  implicit val statusConverter: FieldConverter[String] = new FieldConverter[String]:
    override def fromRowData(rowData: RowData, index: Int): String =
      if rowData.isNullAt(index) then "UNKNOWN"
      else
        val code = rowData.getInt(index)
        code match
          case 1 => "PENDING"
          case 2 => "CONFIRMED"
          case 3 => "SHIPPED"
          case _ => "UNKNOWN"
    
    override def toRowData(value: String, index: Int): Any =
      value match
        case "PENDING" => 1
        case "CONFIRMED" => 2
        case "SHIPPED" => 3
        case _ => 0
    
    override def typeInfo: TypeInformation[String] =
      TypeInformation.of(classOf[String])
```

### Example 4: Semi-Auto Derivation

```scala
import org.apache.flinkx.api.rowdata.semiauto._
import org.apache.flink.api.common.typeinfo.TypeInformation

case class Product(id: String, price: Double, inStock: Boolean)

object Product:
  // Explicit derivation cached in companion object
  // This avoids re-deriving on every use
  given RowDataConverter[Product] = deriveRowDataConverter[Product]
```

## Key Technical Insights

### Comparison: Magnolia vs. Inline Macros

| Aspect | Magnolia (TypeInformation) | Inline (RowDataConverter) |
|--------|---------------------------|---------------------------|
| **Field Derivation** | TaggedDerivation.join() | Mirror.ProductOf extraction |
| **Custom Overrides** | TypeMapper in scope | FieldConverter in scope |
| **Caching** | TrieMap by TypeTag | Compile-time codegen |
| **Error Handling** | Magnolia's derivation errors | Scala compiler errors |
| **Overhead** | Minimal (cached) | Zero (inlined) |
| **Type-Specific Calls** | No | Yes |

### Implicit Resolution Order

When deriving `RowDataConverter[T]` for field type `F`:

1. **Check companion object** for `implicit FieldConverter[F]`
2. **Check caller's scope** for `implicit FieldConverter[F]`
3. **Auto-derive default** using `TypeInformation[F]`

This order ensures custom converters always win.

### Code Generation Strategy

The macro generates unrolled code for efficiency:

```scala
// Instead of (runtime loop):
var i = 0
while i < fieldCount:
  converter(i).fromRowData(row, i)
  i += 1

// Generate (compile-time unrolled):
val field0 = converter0.fromRowData(row, 0)
val field1 = converter1.fromRowData(row, 1)
val field2 = converter2.fromRowData(row, 2)
// ... one line per field
```

This eliminates loop overhead and enables better JIT inlining.

### Type-Specific Accessor Calls

The macro generates the right accessor for each field type:

```scala
// Generated code respects Flink's API design:
val id = if row.isNullAt(0) then null else row.getString(0)  // String
val age = if row.isNullAt(1) then 0 else row.getInt(1)        // Int
val salary = if row.isNullAt(2) then 0.0 else row.getDouble(2) // Double
val active = if row.isNullAt(3) then false else row.getBoolean(3) // Boolean
```

No generic `getField()` with casting - proper type-specific methods.

## Performance Considerations

- ✅ **Zero Reflection**: All field access generated at compile-time
- ✅ **JIT-Friendly**: Specialized code per case class
- ✅ **Inlining**: Small methods can be inlined by JVM
- ✅ **Cache Locality**: Field converters access sequential memory
- ✅ **Type-Specific Calls**: No casting overhead, direct method calls
- ⚠️ **GenericRowData**: Slightly slower than specialized formats
- 🔍 **Benchmark**: Compare against manual implementation

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Macro complexity | Medium | High | Start simple, extensive testing |
| Implicit confusion | Medium | Medium | Clear docs + examples |
| Performance regression | Low | High | Benchmarks before release |
| Scala 2.13 demand | Medium | Low | Document Scala 3 requirement |
| Savepoint incompatibility | Low | High | Integration tests with checkpoints |
| Type-specific accessor calls | Low | Low | Test with actual RowData types |

## Definition of Done

✅ All tests passing (unit + integration + examples)
✅ Zero runtime reflection
✅ Custom converters fully functional
✅ Type-specific accessor calls generated
✅ Documentation complete
✅ Performance on par with manual code
✅ Clean, readable macro implementation
✅ Integrated into CI/CD
✅ Ready for user adoption

## Future Enhancements (Post-Phase 5)

1. **Scala 2.13 Support**: Using typelevel macros
2. **Schema Evolution**: Handle RowData schema changes
3. **Lazy Conversion**: Avoid unnecessary work
4. **Projection**: Select subset of fields
5. **Caching**: Memoize field converters
6. **Async Converters**: For external lookups
7. **Validation**: Built-in field validation
8. **Debugging**: Better error messages and stack traces
