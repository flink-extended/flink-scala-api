# RowData Type-Specific Accessor Methods

## Important Discovery

The Flink `RowData` interface provides **type-specific accessor methods** rather than a generic `getField()` method. This significantly impacts the implementation strategy.

Reference: https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/table/data/RowData.html

## Available Accessor Methods

### Primitive Types
- `getInt(pos: Int): Int`
- `getLong(pos: Int): Long`
- `getFloat(pos: Int): Float`
- `getDouble(pos: Int): Double`
- `getBoolean(pos: Int): Boolean`
- `getShort(pos: Int): Short`
- `getByte(pos: Int): Byte`

### String & Binary
- `getString(pos: Int): StringData`
- `getBinary(pos: Int): byte[]`

### Complex Types
- `getArray(pos: Int): ArrayData`
- `getMap(pos: Int): MapData`
- `getRow(pos: Int): RowData`

### Generic & Utility
- `getField(pos: Int): Any` (type-erased, requires casting)
- `isNullAt(pos: Int): Boolean`

## Design Implications

### 1. FieldConverter Must Be Type-Aware

The `FieldConverter[T]` trait needs to know the field type to call the correct accessor:

```scala
trait FieldConverter[FieldType] extends Serializable {
  // MUST call the right method: getInt, getLong, getString, etc.
  // based on FieldType
  def fromRowData(rowData: RowData, index: Int): FieldType
  
  def toRowData(value: FieldType, index: Int): Any
  def typeInfo: TypeInformation[FieldType]
}
```

### 2. DefaultFieldConverter Implementation

For a `DefaultFieldConverter[T]`, need runtime type matching or compiler-generated code:

```scala
case class DefaultFieldConverter[T](typeInfo: TypeInformation[T]) extends FieldConverter[T] {
  override def fromRowData(rowData: RowData, index: Int): T = {
    if (rowData.isNullAt(index)) null.asInstanceOf[T]
    else {
      // Call type-specific method based on T
      // At compile-time, the macro generates the right call
      // At runtime (if needed), match on typeInfo
      ???
    }
  }
}
```

### 3. Custom Converter Flexibility

Custom `FieldConverter` implementations can leverage type-specific methods:

```scala
case class Event(timestamp: Long) derives RowDataConverter

object Event:
  implicit val timestampConverter: FieldConverter[Long] = 
    new FieldConverter[Long]:
      override def fromRowData(rowData: RowData, index: Int): Long =
        if rowData.isNullAt(index) then 0L
        else rowData.getLong(index) / 1000  // Convert milliseconds to seconds
      
      override def toRowData(value: Long, index: Int): Any = value * 1000
      
      override def typeInfo = TypeInformation.of(classOf[Long])
```

### 4. Writing Back to RowData

**Challenge**: Writing values back to RowData requires type awareness:
- `GenericRowData` has setters: `setField(pos, value)` - generic
- Need to ensure type compatibility
- Should use the inverse of the accessor used for reading

**Solution**: Use `GenericRowData` for simplicity:
```scala
val result = new GenericRowData(fieldCount)
result.setField(index, value)  // Let GenericRowData handle type inference
```

## Macro Implementation Adjustments

### 1. Compile-Time Type-Specific Accessor Generation

The macro should generate code that calls the right accessor at compile time:

```scala
// Example generated code for case class User(id: String, age: Int)
override def fromRowData(row: RowData): User = {
  val id = if row.isNullAt(0) then null else row.getString(0)
  val age = if row.isNullAt(1) then 0 else row.getInt(1)
  User(id, age)
}
```

### 2. Field Type Resolution

At macro expansion time:
1. Extract field types from `Mirror.ProductOf[T]`
2. Map each field type to the corresponding RowData accessor
3. Generate unrolled code with direct accessor calls

### 3. Custom Converters Override

When a custom `FieldConverter` is provided, it handles accessor selection:

```scala
// User-defined custom converter handles its own accessor calls
implicit val customConverter: FieldConverter[Long] = new FieldConverter[Long]:
  def fromRowData(rowData: RowData, index: Int): Long =
    if rowData.isNullAt(index) then 0L else rowData.getLong(index) / 1000
```

The macro just calls: `customConverter.fromRowData(row, index)` - no need to know the accessor logic.

## Type-to-Accessor Mapping

For macro code generation:

| Scala Type | RowData Accessor | RowData Setter | Notes |
|------------|------------------|----------------|-------|
| `Int` | `getInt(i)` | `setInt(i, v)` | Primitive |
| `Long` | `getLong(i)` | `setLong(i, v)` | Primitive |
| `Float` | `getFloat(i)` | `setFloat(i, v)` | Primitive |
| `Double` | `getDouble(i)` | `setDouble(i, v)` | Primitive |
| `Boolean` | `getBoolean(i)` | `setBoolean(i, v)` | Primitive |
| `Short` | `getShort(i)` | `setShort(i, v)` | Primitive |
| `Byte` | `getByte(i)` | `setByte(i, v)` | Primitive |
| `String` | `getString(i)` | `setField(i, v)` | StringData |
| `Array[Byte]` | `getBinary(i)` | `setField(i, v)` | Binary |
| `List/Seq` | `getArray(i)` | `setField(i, v)` | ArrayData |
| `Map` | `getMap(i)` | `setField(i, v)` | MapData |
| Custom case class | `getRow(i)` | `setField(i, v)` | Nested RowData |
| Other | `getField(i)` | `setField(i, v)` | Fallback with casting |

## Testing Implications

### Unit Tests Must Cover

1. ✅ All primitive type accessors work correctly
2. ✅ Null checks with `isNullAt()` prevent NPE
3. ✅ String/Binary types handled
4. ✅ Complex types (ArrayData, MapData, nested RowData)
5. ✅ GenericRowData setField works for round-trip
6. ✅ Custom converters can override accessor behavior
7. ✅ Type-specific accessors are called (not generic getField)

### Integration Tests

1. ✅ Round-trip with real Flink RowData from Table API
2. ✅ Compatibility with different RowData implementations
3. ✅ Performance comparison with manual accessor calls
4. ✅ Macro-generated code calls right accessors

## Decision: Use Type-Specific Accessors

**Recommendation**: The macro should generate code that calls the type-specific accessor methods when possible (at compile time) rather than relying on generic `getField()` with casting.

**Benefits**:
- ✅ Follows Flink's API design intent
- ✅ Avoids unnecessary type casting
- ✅ Better performance (fewer conversions)
- ✅ Type-safe at compile time
- ✅ Clearer generated code

**Implementation Approach**:

1. **At compile time**: Use Mirror type information to determine the accessor
   - `String` → `getString(index)`
   - `Int` → `getInt(index)`
   - `Long` → `getLong(index)`
   - etc.

2. **For unknown types**: Fall back to generic `getField()` with unsafe cast

3. **For custom converters**: Let the user implementation decide

## Example: Generated Code Shape

```scala
// Input: case class MyEvent(id: String, count: Int, timestamp: Long)

// Generated fromRowData implementation:
override def fromRowData(row: RowData): MyEvent = {
  // Field 0: String
  val id = if row.isNullAt(0) then null else row.getString(0)
  
  // Field 1: Int (with custom converter)
  val count = customCountConverter.fromRowData(row, 1)
  
  // Field 2: Long
  val timestamp = if row.isNullAt(2) then 0L else row.getLong(2)
  
  MyEvent(id, count, timestamp)
}

// Generated toRowData implementation:
override def toRowData(value: MyEvent): RowData = {
  val result = new GenericRowData(3)
  result.setField(0, value.id)
  result.setField(1, customCountConverter.toRowData(value.count, 1))
  result.setField(2, value.timestamp)
  result
}
```

This is:
- ✅ Type-safe
- ✅ No unnecessary casting
- ✅ Efficient
- ✅ Follows Flink conventions
