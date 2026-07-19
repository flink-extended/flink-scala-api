# Implementation Plan: RowData to Scala Case Class Converter

## Executive Summary

Add functionality to convert Flink `RowData` records to user Scala case classes using Scala 3's `derives` macro, with support for custom per-field conversion functions. Position-based field mapping ensures RowData column indices match case class field declaration order.

**Key Innovation**: Support implicit `FieldConverter[T]` overrides per field, allowing users to define custom logic (enum conversion, unit conversion, etc.) while the macro handles the plumbing.

---

## Design Pattern (Following Existing Codebase)

This implementation mirrors the existing `TypeInformation` derivation pattern:

```
TypeInformationDerivation (uses Magnolia)
    ├─ auto.scala (automatic via given)
    └─ semiauto.scala (explicit deriveTypeInformation)

RowDataConverterDerivation (will use inline + Mirror)
    ├─ auto.scala (automatic via derives or given)
    └─ semiauto.scala (explicit deriveRowDataConverter)
```

**Why not pure Magnolia?** The `derives` clause requires inline support for compile-time field extraction and custom converter resolution. Inline macros provide cleaner implementation than wrapping Magnolia.

---

## Phase 1: Foundation (Core Data Structures)

### 1.1 `FieldConverter[T]` Trait
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/FieldConverter.scala`

Represents conversion logic for a single field at a specific RowData position.

```scala
package org.apache.flinkx.api.rowdata

import org.apache.flink.types.RowData
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * Converts a RowData column (at a specific index) to/from a Scala field type.
 *
 * Users implement this trait to define custom conversion logic.
 * Position index enables cross-field lookups if needed.
 *
 * @tparam FieldType The target Scala field type
 */
trait FieldConverter[FieldType] extends Serializable {
  
  /**
   * Read from RowData at position `index` and convert to FieldType.
   * 
   * @param rowData the source RowData record
   * @param index column position in RowData (matches case class field order)
   */
  def fromRowData(rowData: RowData, index: Int): FieldType
  
  /**
   * Convert a FieldType value back to RowData-compatible format.
   * 
   * @param value the field value
   * @param index column position (informational for context)
   */
  def toRowData(value: FieldType, index: Int): Any
  
  /**
   * TypeInformation for Flink integration.
   * Must match FieldType.
   */
  def typeInfo: TypeInformation[FieldType]
}
```

**Key Design Decisions**:
- ✅ Serializable for distributed execution (checkpointing)
- ✅ Index parameter enables advanced use cases (cross-field conversions)
- ✅ Simple, focused interface
- ✅ Mirrors `MappedSerializer.TypeMapper` pattern

**Testing**:
- [ ] Trait is compilable
- [ ] Can be extended by user code
- [ ] Serialization works

---

### 1.2 `DefaultFieldConverter[T]` Implementation
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/DefaultFieldConverter.scala`

Fallback converter when no custom implementation provided.

```scala
package org.apache.flinkx.api.rowdata

import org.apache.flink.types.RowData
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * Default converter: direct read/write without transformation.
 * Used when no implicit FieldConverter[T] is in scope.
 */
case class DefaultFieldConverter[T](typeInfo: TypeInformation[T]) extends FieldConverter[T] {
  
  override def fromRowData(rowData: RowData, index: Int): T =
    if rowData.isNullAt(index) then null.asInstanceOf[T]
    else rowData.getField(index).asInstanceOf[T]
  
  override def toRowData(value: T, index: Int): Any = value
}
```

**Key Points**:
- ✅ Handles null fields correctly
- ✅ Direct cast (assumes Type Erasure handled by TypeInformation)
- ✅ No transformation overhead
- ✅ Reuses existing TypeInformation from implicit scope

**Testing**:
- [ ] Primitives (Int, String, Long, Double, Boolean)
- [ ] Collections (List, Set, Map)
- [ ] Null field handling
- [ ] Case class fields

---

### 1.3 `RowDataConverter[T]` Main Trait
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/RowDataConverter.scala`

The public API that users depend on.

```scala
package org.apache.flinkx.api.rowdata

import org.apache.flink.types.RowData
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * Converts between RowData records and Scala case classes.
 *
 * Field positions in RowData are based on case class field declaration order.
 * Custom converters can be provided via implicit FieldConverter[T] in scope.
 *
 * Usage (automatic):
 * {{{
 *   import org.apache.flinkx.api.rowdata.auto._
 *   case class User(id: String, name: String) derives RowDataConverter
 * }}}
 *
 * Usage (with custom field converter):
 * {{{
 *   case class Event(userId: String, timestamp: Long) derives RowDataConverter
 *   
 *   object Event:
 *     implicit val timestampConverter: FieldConverter[Long] = ...
 * }}}
 */
trait RowDataConverter[T] extends Serializable {
  
  /** Convert RowData → case class instance */
  def fromRowData(row: RowData): T
  
  /** Convert case class instance → RowData */
  def toRowData(value: T): RowData
  
  /** TypeInformation for this type (for Flink integration) */
  def typeInfo: TypeInformation[T]
}
```

**Key Design Decisions**:
- ✅ Serializable for checkpointing
- ✅ Three methods: from/to RowData, typeInfo
- ✅ Focuses on the conversion contract
- ✅ Macro handles implementation generation

**Testing**:
- [ ] Round-trip: RowData → T → RowData
- [ ] Null handling
- [ ] Collection fields

---

## Phase 2: Macro Engine (Core Derivation)

### 2.1 `RowDataConverterDerivation` Inline Macro
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterDerivation.scala`

This is the complex part. Generates optimized converter implementations at compile time.

```scala
package org.apache.flinkx.api.rowdata

import scala.deriving.Mirror
import scala.compiletime.{constValue, erasedValue, summonInline}
import org.apache.flink.types.RowData
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import scala.reflect.ClassTag

/**
 * Derivation of RowDataConverter using Scala 3 inline macros.
 *
 * Algorithm:
 * 1. Extract field names and types from Mirror.ProductOf[T]
 * 2. For each field at index i:
 *    a. Try to summon FieldConverter[FieldType] from implicit scope
 *    b. If found, use it
 *    c. If not, derive DefaultFieldConverter using TypeInformation
 * 3. Generate specialized RowDataConverter implementation
 * 4. Aggregate TypeInformation from all field converters
 */
private[rowdata] object RowDataConverterDerivation:
  
  inline def derived[T](using inline m: Mirror.ProductOf[T]): RowDataConverter[T] =
    deriveImpl[T]
  
  private inline def deriveImpl[T](using inline m: Mirror.ProductOf[T]): RowDataConverter[T] = {
    // Extract field count and names
    inline val fieldCount: Int = constValue[Tuple.Size[m.MirroredElemTypes]]
    inline val fieldNames: List[String] = ???  // from Mirror labels
    inline val fieldTypes: List[TypeInformation[?]] = ???  // summon from implicit scope
    
    // Build the converter instance
    // This creates anonymous class implementing RowDataConverter[T]
    new RowDataConverter[T]:
      override def fromRowData(row: RowData): T =
        val fields = new Array[Any](fieldCount)
        // Unrolled: fields(0) = converter0.fromRowData(row, 0)
        //          fields(1) = converter1.fromRowData(row, 1)
        //          ...
        m.fromProduct(???).asInstanceOf[T]
      
      override def toRowData(value: T): RowData =
        val result = new GenericRowData(fieldCount)
        // Unrolled: result.setField(0, converter0.toRowData(value.field0, 0))
        //          result.setField(1, converter1.toRowData(value.field1, 1))
        //          ...
        result
      
      override def typeInfo: TypeInformation[T] =
        new RowTypeInfo(fieldTypes.toArray, fieldNames.toArray).asInstanceOf[TypeInformation[T]]
  }
```

**Implementation Strategy**:
- Use `scala.compiletime.summonInline` to resolve `FieldConverter[T]` per field
- Use `scala.deriving.Mirror` for field extraction
- Generate unrolled loop code (not actual loops) for best performance
- Use `GenericRowData` for RowData representation

**Key Challenges**:
1. **Field Extraction**: Need to map Mirror labels to actual field names
2. **Tuple Unpacking**: Extract individual field types from `MirroredElemTypes`
3. **Error Messages**: Make compile errors clear when RowDataConverter can't be derived
4. **Performance**: Generate specialized code, not reflection-heavy paths

**Macro Implementation Pattern** (simplified outline):

```scala
// In summonAll for all fields, try to summon FieldConverter or default
inline def summonFieldConverters[T <: Tuple]: List[FieldConverter[?]] =
  inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (head *: tail) =>
      val headConverter = summonInline[FieldConverter[head]] match
        case fc: FieldConverter[head] => fc
        case _ =>
          val ti = summonInline[TypeInformation[head]]
          DefaultFieldConverter(ti)
      headConverter :: summonFieldConverters[tail]
```

**Testing**:
- [ ] Single field case class compiles and works
- [ ] Multi-field case class works
- [ ] Custom converter overrides default
- [ ] Multiple custom converters work
- [ ] Null fields handled
- [ ] Compile errors are clear
- [ ] Generated code is efficient

---

### 2.2 Auto/SemiAuto Derivation Traits
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/auto.scala`
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/semiauto.scala`

Following the existing `TypeInformation` pattern.

**auto.scala**:
```scala
package org.apache.flinkx.api.rowdata

import scala.deriving.Mirror

/**
 * Automatic derivation of RowDataConverter.
 * 
 * Import and use with `derives` clause:
 * {{{
 *   import org.apache.flinkx.api.rowdata.auto._
 *   case class User(id: String, name: String) derives RowDataConverter
 * }}}
 */
object auto:
  given deriveRowDataConverter[T](using 
    inline m: Mirror.ProductOf[T]
  ): RowDataConverter[T] =
    RowDataConverterDerivation.derived[T]
```

**semiauto.scala**:
```scala
package org.apache.flinkx.api.rowdata

import scala.deriving.Mirror

/**
 * Semi-automatic (explicit) derivation of RowDataConverter.
 * 
 * Use for explicit control and compile-time caching:
 * {{{
 *   import org.apache.flinkx.api.rowdata.semiauto._
 *   
 *   case class User(id: String, name: String)
 *   
 *   object User:
 *     given RowDataConverter[User] = deriveRowDataConverter[User]
 * }}}
 */
object semiauto:
  inline def deriveRowDataConverter[T](using
    inline m: Mirror.ProductOf[T]
  ): RowDataConverter[T] =
    RowDataConverterDerivation.derived[T]
```

**Key Points**:
- ✅ Follows existing TypeInformation pattern
- ✅ auto: implicit `given` for automatic resolution
- ✅ semiauto: explicit method for caching in companion object
- ✅ Both delegate to same `RowDataConverterDerivation.derived`

**Testing**:
- [ ] auto derivation with derives clause
- [ ] semiauto with explicit call in companion
- [ ] Both work identically
- [ ] Custom converters available in both modes

---

## Phase 3: User Experience (Extensions & Exports)

### 3.1 Extension Methods
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/extensions.scala`

Ergonomic syntax sugar.

```scala
package org.apache.flinkx.api.rowdata

import org.apache.flink.types.RowData

/**
 * Extension methods for seamless RowData ↔ case class conversion.
 */
extension [T](rowData: RowData)(using converter: RowDataConverter[T])
  /**
   * Convert RowData to case class instance.
   * 
   * Usage: rowData.toScala[User]
   */
  def toScala: T = converter.fromRowData(rowData)

extension [T](value: T)(using converter: RowDataConverter[T])
  /**
   * Convert case class to RowData.
   * 
   * Usage: user.toRowData
   */
  def toRowData: RowData = converter.toRowData(value)
```

**Testing**:
- [ ] Extension methods resolve correctly
- [ ] Type inference works
- [ ] Chaining operations

### 3.2 Package Exports
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/package.scala`

```scala
package org.apache.flinkx.api.rowdata

export FieldConverter
export DefaultFieldConverter
export RowDataConverter
export RowDataConverterDerivation
export extensions.*
```

**Testing**:
- [ ] All exports resolve
- [ ] No circular dependencies

---

## Phase 4: Real-World Examples

### 4.1 Basic Example
**File**: `modules/examples/src/main/scala-3/examples/rowdata/BasicRowDataExample.scala`

```scala
import org.apache.flinkx.api.rowdata.auto._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.data.GenericRowData

case class User(id: String, name: String, age: Int) derives RowDataConverter

object BasicRowDataExample:
  def main(args: Array[String]): Unit =
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    val rowDataStream = env.fromData(
      GenericRowData(3, "user1", "Alice", 30),
      GenericRowData(3, "user2", "Bob", 25)
    )
    
    val userStream = rowDataStream.map { rowData =>
      rowData.toScala[User]
    }
    
    userStream.print()
    env.execute()
```

### 4.2 Custom Converter Example
**File**: `modules/examples/src/main/scala-3/examples/rowdata/CustomConverterExample.scala`

```scala
import org.apache.flinkx.api.rowdata.auto._
import org.apache.flinkx.api.rowdata.FieldConverter
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.RowData
import java.time.Instant

case class Event(
  userId: String,
  timestamp: Long,  // Will use custom converter
  eventType: String
) derives RowDataConverter

object Event:
  // Custom converter for timestamp: convert milliseconds to seconds
  implicit val timestampConverter: FieldConverter[Long] = new FieldConverter[Long]:
    override def fromRowData(rowData: RowData, index: Int): Long =
      (rowData.getField(index).asInstanceOf[Long]) / 1000
    
    override def toRowData(value: Long, index: Int): Any =
      value * 1000
    
    override def typeInfo: TypeInformation[Long] =
      TypeInformation.of(classOf[Long])
```

### 4.3 Enum Conversion Example
**File**: `modules/examples/src/main/scala-3/examples/rowdata/EnumConversionExample.scala`

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
  // Convert between status codes and strings
  implicit val statusConverter: FieldConverter[String] = new FieldConverter[String]:
    override def fromRowData(rowData: RowData, index: Int): String =
      val code = rowData.getField(index).asInstanceOf[Int]
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

---

## Phase 5: Testing Strategy

### 5.1 Unit Tests
**File**: `modules/flink-common-api/src/test/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterSpec.scala`

```scala
class RowDataConverterSpec extends AnyWordSpec with Matchers:
  
  "RowDataConverter" should:
    "derive for single field case class" in:
      case class Single(value: String) derives RowDataConverter
      ???
    
    "derive for multi-field case class" in:
      case class Multi(id: String, age: Int) derives RowDataConverter
      ???
    
    "use custom FieldConverter when in scope" in:
      case class Event(timestamp: Long) derives RowDataConverter
      object Event:
        implicit val ts: FieldConverter[Long] = ???
      ???
    
    "handle null fields correctly" in:
      ???
    
    "support extension methods" in:
      ???
    
    "round-trip conversion preserves data" in:
      ???
```

### 5.2 Integration Tests
**File**: `modules/flink-common-api/src/test/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterIntegrationSpec.scala`

```scala
class RowDataConverterIntegrationSpec extends AnyWordSpec:
  
  "RowDataConverter" should:
    "work with StreamExecutionEnvironment" in:
      ???
    
    "work with Table API" in:
      ???
    
    "support savepoint/restore" in:
      ???
    
    "perform efficiently" in:
      ???
```

---

## Implementation Checklist

### Phase 1 (Week 1)
- [ ] Create `FieldConverter` trait
- [ ] Create `DefaultFieldConverter` implementation
- [ ] Create `RowDataConverter` trait
- [ ] Write unit tests for each
- [ ] Verify serialization

### Phase 2 (Week 2)
- [ ] Design inline macro algorithm
- [ ] Implement `RowDataConverterDerivation.derived`
- [ ] Implement field converter summoning
- [ ] Handle implicit resolution precedence
- [ ] Write comprehensive macro tests
- [ ] Test error messages

### Phase 3 (Week 2.5)
- [ ] Create `auto.scala`
- [ ] Create `semiauto.scala`
- [ ] Create extension methods
- [ ] Create package exports
- [ ] Integration tests

### Phase 4 (Week 3)
- [ ] Write 4 code examples
- [ ] Test all examples compile and run
- [ ] Document example usage

### Phase 5 (Week 3.5)
- [ ] Full test suite
- [ ] Performance benchmarks
- [ ] Edge case testing
- [ ] Null handling verification

### Documentation (Week 4)
- [ ] Update main README
- [ ] Add scaladoc comments
- [ ] Create migration guide
- [ ] Performance tuning tips

---

## Key Technical Insights

### Comparison: Magnolia vs. Inline Macros

| Aspect | Magnolia (TypeInformation) | Inline (RowDataConverter) |
|--------|---------------------------|--------------------------|
| **Field Derivation** | TaggedDerivation.join() | Mirror.ProductOf extraction |
| **Custom Overrides** | TypeMapper in scope | FieldConverter in scope |
| **Caching** | TrieMap by TypeTag | Compile-time codegen |
| **Error Handling** | Magnolia's derivation errors | Scala compiler errors |
| **Overhead** | Minimal (cached) | Zero (inlined) |

### Implicit Resolution Order

When deriving `RowDataConverter[T]` for field type `F`:

1. **Check companion object** for `implicit FieldConverter[F]`
2. **Check caller's scope** for `implicit FieldConverter[F]`
3. **Auto-derive default** using `TypeInformation[F]`

This order ensures custom converters always win.

### Code Generation Strategy

The macro generates unrolled code like:

```scala
// Instead of:
var i = 0; while i < fieldCount: converter(i).fromRowData(row, i)

// Generate:
val field0 = converter0.fromRowData(row, 0)
val field1 = converter1.fromRowData(row, 1)
// ... unrolled for each field
```

This eliminates loop overhead and enables JIT optimization.

---

## Performance Considerations

- ✅ **Zero Reflection**: All field access generated at compile-time
- ✅ **JIT-Friendly**: Specialized code per case class
- ✅ **Inlining**: Small methods can be inlined by JVM
- ✅ **Cache Locality**: Field converters access sequential memory
- ⚠️ **GenericRowData**: Slightly slower than specialized formats
- 🔍 **Benchmark**: Compare against manual implementation

---

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Macro complexity | Medium | High | Start simple, extensive testing |
| Implicit confusion | Medium | Medium | Clear docs + examples |
| Performance regression | Low | High | Benchmarks before release |
| Scala 2.13 demand | Medium | Low | Document Scala 3 requirement |
| Savepoint incompatibility | Low | High | Integration tests with checkpoints |

---

## Definition of Done

✅ All tests passing (unit + integration + examples)
✅ Zero runtime reflection
✅ Custom converters fully functional
✅ Documentation complete
✅ Performance on par with manual code
✅ Clean, readable macro implementation
✅ Integrated into CI/CD
✅ Ready for user adoption

---

## File Structure

```
modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/
├── FieldConverter.scala                    # Core trait
├── DefaultFieldConverter.scala             # Default implementation
├── RowDataConverter.scala                  # Main API
├── RowDataConverterDerivation.scala        # Macro engine [COMPLEX]
├── auto.scala                              # Automatic derivation
├── semiauto.scala                          # Explicit derivation
├── extensions.scala                        # Extension methods
└── package.scala                           # Exports

modules/flink-common-api/src/test/scala-3/org/apache/flinkx/api/rowdata/
├── RowDataConverterSpec.scala              # Unit tests
└── RowDataConverterIntegrationSpec.scala   # Integration tests

modules/examples/src/main/scala-3/examples/rowdata/
├── BasicRowDataExample.scala
├── CustomConverterExample.scala
├── EnumConversionExample.scala
└── UnitConversionExample.scala
```

---

## Next Steps

1. ✅ Review this implementation plan
2. 🎯 Approve design decisions
3. 🚀 Begin Phase 1 (foundation)
4. 📊 Create JIRA/GitHub issues if needed
5. 📅 Schedule review checkpoints

