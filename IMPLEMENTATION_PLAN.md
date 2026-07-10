# Implementation Plan: RowData to Scala Case Class Converter with Custom Field Functions

## Overview
Add support for converting Flink `RowData` records to user case classes using Scala 3's `derives` macro with compile-time safety and support for per-field custom conversion logic.

## Goals
1. ✅ Zero runtime reflection via Scala 3 inline macros
2. ✅ Position-based field mapping (RowData index = case class field index)
3. ✅ Support custom `FieldConverter[T]` functions per field
4. ✅ Automatic and semi-automatic derivation modes
5. ✅ Seamless integration with existing Magnolia TypeInformation framework
6. ✅ Compile-time type safety
7. ✅ Extension methods for ergonomics
8. ✅ Comprehensive test coverage

## Phase 1: Core Infrastructure (Foundation)

### 1.1 Create FieldConverter Trait
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/FieldConverter.scala`
**Complexity**: Low
**Dependencies**: None

Core trait defining per-field conversion logic:
- `fromRowData(rowData: RowData, index: Int): T` - Read from RowData at position
- `toRowData(value: T, index: Int): Any` - Write back to RowData
- `typeInfo: TypeInformation[T]` - Flink type metadata

**Key Points**:
- Serializable for distributed execution
- Index parameter enables cross-field access if needed
- Position-based mapping is fundamental design

**Testing**:
- Unit tests for trait interface
- Verify serialization of implementations

### 1.2 Create DefaultFieldConverter
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/DefaultFieldConverter.scala`
**Complexity**: Low
**Dependencies**: 1.1

Default implementation using existing TypeInformation:
- Simple passthrough: read from RowData index, write as-is
- No transformation logic
- Fallback for types without custom converters

**Key Points**:
- Reuses existing TypeInformation from implicit scope
- Pattern matches on RowData.getField() result

**Testing**:
- Primitive types (Int, String, Long, Double, Boolean)
- Collections (List, Set, Map)
- Null handling

### 1.3 Create FieldMetadata
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/FieldMetadata.scala`
**Complexity**: Medium
**Dependencies**: 1.1, 1.2

Metadata holder for case class fields:
- `index: Int` - Position in both case class and RowData
- `fieldName: String` - For debugging/reflection (optional)
- `converter: FieldConverter[T]` - The field's custom converter

**Key Points**:
- Generic over field type T
- Carries all needed info for conversion
- Will be used in macro-generated code

**Testing**:
- Type safety with various field types
- Index ordering verification

---

## Phase 2: Core Converter (Main Logic)

### 2.1 Create RowDataConverter Trait
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/RowDataConverter.scala`
**Complexity**: Medium
**Dependencies**: 1.1, 1.3

Main public API trait:
- `fromRowData(row: RowData): T` - RowData → case class
- `toRowData(value: T): RowData` - case class → RowData
- `typeInfo: TypeInformation[T]` - For integration

**Key Points**:
- Serializable for Flink checkpointing
- Abstracts away field-level conversion details
- Single entry point for users

**Testing**:
- Round-trip conversion: RowData → case class → RowData
- Null field handling
- Type preservation

### 2.2 Inline Macro for RowDataConverter Derivation
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterDerivation.scala`
**Complexity**: High
**Dependencies**: 2.1, 1.1-1.3

Core macro implementation using `inline def derived[T]`:

**Algorithm**:
1. Use `Mirror.ProductOf[T]` to extract field information
2. For each field at index `i`:
   - Look for implicit `FieldConverter[FieldType]` in scope
   - If found, use it
   - Otherwise, derive default using `TypeInformation`
3. Generate specialized implementation class (not generic)
4. Return instance

**Key Points**:
- **Scala 3 inline macros** (not Magnolia, different approach)
- Compile-time field discovery
- Implicit resolution follows Scala's precedence rules
- Custom converters in companion object override defaults
- No runtime reflection

**Code Structure**:
```scala
inline def derived[T](using inline m: Mirror.ProductOf[T]): RowDataConverter[T] = {
  inline m match
    case p: Mirror.ProductOf[T] =>
      deriveImpl[T](p)
}

private inline def deriveImpl[T](using inline m: Mirror.ProductOf[T]): RowDataConverter[T] = {
  // Generate class implementing RowDataConverter[T]
  // Unroll field conversions at compile time
  ???
}
```

**Testing**:
- Single field case class
- Multiple field case classes
- Custom converter precedence
- Type error messages
- Compile-time verification

### 2.3 Create Auto/SemiAuto Modules
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/auto.scala`
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/semiauto.scala`
**Complexity**: Medium
**Dependencies**: 2.1, 2.2

**auto.scala**:
```scala
trait auto extends RowDataConverterDerivation:
  // Auto-derive given when RowDataConverter is needed
  given deriveRowDataConverter[T](using inline m: Mirror.ProductOf[T]): RowDataConverter[T] =
    derived[T]
```

**semiauto.scala**:
```scala
trait semiauto extends RowDataConverterDerivation:
  // Only export derived method, user must call explicitly
  inline def deriveRowDataConverter[T](using 
    inline m: Mirror.ProductOf[T]
  ): RowDataConverter[T] = derived[T]
```

**Key Points**:
- Follow existing TypeInformation pattern
- auto: implicit resolution (convenient)
- semiauto: explicit (cacheable in companion objects)
- Both live in separate objects/traits

**Testing**:
- Derive with `given` keyword
- Explicit companion object derivation
- Implicit precedence ordering

---

## Phase 3: Integration & Extensions

### 3.1 Extension Methods for Ergonomics
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/RowDataExtensions.scala`
**Complexity**: Low
**Dependencies**: 2.1

Provide convenient syntax:
```scala
extension [T](rowData: RowData)(using converter: RowDataConverter[T])
  def toScala: T = converter.fromRowData(rowData)

extension [T](value: T)(using converter: RowDataConverter[T])
  def toRowData: RowData = converter.toRowData(value)
```

**Testing**:
- Extension method resolution
- Type inference
- Chaining

### 3.2 Package.scala Exports
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/package.scala`
**Complexity**: Low
**Dependencies**: All Phase 1-3

Re-export public API:
- `FieldConverter`
- `DefaultFieldConverter`
- `RowDataConverter`
- Extension methods

**Testing**:
- Import statements resolve correctly

### 3.3 Integration with Existing Magnolia Framework (Optional for Phase 1)
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterFromTypeInfo.scala`
**Complexity**: Medium
**Dependencies**: 2.1, existing TypeInformationDerivation

Allow deriving RowDataConverter from existing TypeInformation:
- Useful for types already have TypeInformation
- Bridges gap between two systems

**Testing**:
- Interop with TypeInformation[T]

---

## Phase 4: Test Suite

### 4.1 Unit Tests
**File**: `modules/flink-common-api/src/test/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterSpec.scala`
**Coverage**:
- ✅ Auto derivation with defaults
- ✅ Custom single field converter
- ✅ Custom multiple field converters
- ✅ Custom precedence (companion object > local scope > default)
- ✅ Null field handling
- ✅ Primitive types
- ✅ Collection types
- ✅ Optional types
- ✅ Nested case classes
- ✅ Extension method resolution

### 4.2 Integration Tests
**File**: `modules/flink-common-api/src/test/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterIntegrationSpec.scala`
**Coverage**:
- ✅ Round-trip: RowData → case class → RowData
- ✅ With Flink StreamExecutionEnvironment
- ✅ With Flink Table API (GenericRowData)
- ✅ Savepoint compatibility
- ✅ Cross-field converter access (index parameter)

### 4.3 Example Tests
**File**: `modules/examples/src/main/scala-3/org/apache/flinkx/examples/rowdata/RowDataConverterExample.scala`
**Coverage**:
- ✅ Basic usage example
- ✅ Custom converter example
- ✅ Enum conversion example
- ✅ Unit conversion example
- ✅ Real Flink job example

---

## Phase 5: Documentation & Examples

### 5.1 README Examples
**Location**: Update main `README.md` section on RowData conversion

**Content**:
- Basic usage
- Custom converter example
- Implicit resolution explanation
- Comparison with TypeInformation
- Performance considerations

### 5.2 Code Examples
**File**: `modules/examples/src/main/scala-3/org/apache/flinkx/examples/rowdata/`
**Files**:
- `BasicRowDataConversion.scala` - Simple example
- `CustomFieldConverters.scala` - Custom logic example
- `EnumConversion.scala` - Enum-like conversion
- `UnitConversion.scala` - Unit conversion (ms ↔ s)
- `ComplexNestedTypes.scala` - Advanced example

### 5.3 API Documentation
**Scaladoc Comments**:
- All public classes/traits
- Usage examples in docstrings
- Cross-references to related types

---

## Implementation Sequence

### Week 1: Foundation
1. ✅ 1.1 FieldConverter trait
2. ✅ 1.2 DefaultFieldConverter
3. ✅ 1.3 FieldMetadata
4. 🧪 Unit tests for each

### Week 2: Core Engine
5. ✅ 2.1 RowDataConverter trait
6. ✅ 2.2 Inline macro derivation (hardest part)
7. 🧪 Comprehensive macro tests
8. 🧪 Edge case tests

### Week 3: Polish & Integration
9. ✅ 2.3 Auto/SemiAuto modules
10. ✅ 3.1 Extension methods
11. ✅ 3.2 Package exports
12. 🧪 Integration tests with Flink jobs

### Week 4: Documentation & Examples
13. 📝 README updates
14. 📝 Code examples
15. 📝 Scaladoc comments
16. 🧪 Example tests

---

## Key Technical Decisions

### 1. **Inline Macros vs. Magnolia**
- **Decision**: Use Scala 3 inline macros for derives, not Magnolia
- **Reason**: 
  - `derives` clause requires inline support
  - Simpler for field-level customization
  - Magnolia still used for TypeInformation derivation (separate concern)
  - Better compile-time error messages for derives
- **Trade-off**: Macro complexity, but clearer API

### 2. **Position-Based vs. Name-Based Mapping**
- **Decision**: Position-based (RowData index = field index)
- **Reason**:
  - Matches Flink Table API convention
  - Simpler, faster, no name lookup overhead
  - More predictable for generated code
- **Limitation**: Field order matters (but natural in case classes)

### 3. **FieldConverter Scope Resolution**
- **Priority Order**:
  1. Explicit `FieldConverter[T]` in caller's scope (companion object wins)
  2. Local scope implicit
  3. Auto-derived default using `TypeInformation`
- **Reason**: Follows Scala implicit resolution rules
- **Benefit**: Predictable, users can override selectively

### 4. **Serialization Strategy**
- **Decision**: Make converters Serializable for distributed execution
- **Implementation**: Case classes + explicit serializers
- **Testing**: Verify with Flink checkpointing

### 5. **Scala 3 Only**
- **Decision**: RowData conversion is Scala 3 only (for now)
- **Reason**: Requires `derives` macro and inline capabilities
- **Future**: Could add Scala 2.13 support with typelevel/scala 2.13 macro annotations

---

## Testing Strategy

### Unit Tests Checklist
- [ ] FieldConverter trait compiles and implements correctly
- [ ] DefaultFieldConverter handles all primitive types
- [ ] RowDataConverter trait interface is sound
- [ ] Macro derivation generates correct code
- [ ] Custom converters override defaults
- [ ] Multiple custom converters work correctly
- [ ] Null/Optional fields handled
- [ ] Nested case classes work
- [ ] Collections (List, Set, Map) work
- [ ] Extension methods resolve correctly
- [ ] Round-trip conversion is identity-preserving
- [ ] Serialization/deserialization works

### Integration Tests Checklist
- [ ] Works with StreamExecutionEnvironment
- [ ] Works with Table API
- [ ] Works with GenericRowData
- [ ] Compatible with Flink checkpointing
- [ ] Performance acceptable (no unexpected slowdowns)
- [ ] Cross-field access patterns work
- [ ] Complex real-world example job runs successfully

### Error Handling Checklist
- [ ] Compile error when RowDataConverter not derivable
- [ ] Runtime error with good message when field missing
- [ ] Runtime error with good message on type mismatch
- [ ] Null field handling doesn't crash

---

## Risks & Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| Macro complexity | Medium | High | Start with simple cases, extensive testing |
| Implicit resolution confusion | Medium | Medium | Clear docs, examples, compile errors |
| Performance regression | Low | High | Benchmarks against manual code |
| Scala 2.13 users want feature | Medium | Medium | Document Scala 3-only, plan future backport |
| Cross-field access is confusing | Low | Medium | Clear examples, document limitations |
| Savepoint compatibility issues | Low | High | Thorough integration tests |

---

## Success Criteria

✅ All tests passing (unit + integration + examples)
✅ Zero runtime reflection 
✅ Compile-time errors for invalid types
✅ Custom converters fully working
✅ Documentation complete with examples
✅ Performance on par with manual conversion
✅ Clean, maintainable macro code
✅ Integrated into CI/CD pipeline
✅ Ready for user adoption

---

## Future Enhancements (Post-Phase 5)

1. **Scala 2.13 Support**: Using typelevel macros
2. **Schema Evolution**: Handle RowData schema changes
3. **Lazy Conversion**: Avoid unnecessary work
4. **Projection**: Select subset of fields
5. **Caching**: Memoize field converters
6. **Async Converters**: For external lookups
7. **Validation**: Built-in field validation
8. **Debugging**: Better error messages and stack traces

---

## File Structure Summary

```
modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/
├── FieldConverter.scala              (Phase 1.1)
├── DefaultFieldConverter.scala       (Phase 1.2)
├── FieldMetadata.scala               (Phase 1.3)
├── RowDataConverter.scala            (Phase 2.1)
├── RowDataConverterDerivation.scala  (Phase 2.2) [COMPLEX MACRO]
├── RowDataExtensions.scala           (Phase 3.1)
├── package.scala                     (Phase 3.2)
├── auto.scala                        (Phase 2.3)
└── semiauto.scala                    (Phase 2.3)

modules/flink-common-api/src/test/scala-3/org/apache/flinkx/api/rowdata/
├── RowDataConverterSpec.scala        (Phase 4.1)
└── RowDataConverterIntegrationSpec.scala (Phase 4.2)

modules/examples/src/main/scala-3/org/apache/flinkx/examples/rowdata/
├── BasicRowDataConversion.scala      (Phase 5.2)
├── CustomFieldConverters.scala       (Phase 5.2)
├── EnumConversion.scala              (Phase 5.2)
└── UnitConversion.scala              (Phase 5.2)
```

---

## Notes for Implementer

1. **Start with 1.1-1.3**: Get basic structure solid before macro work
2. **2.2 is complex**: Budget extra time for inline macro development
3. **Testing is critical**: Especially for macro-generated code
4. **Learn from TypeInformationDerivation.scala**: It's a working example of inline macros in this codebase
5. **Compile errors matter**: Invest in good error messages
6. **Document assumptions**: Comment on why decisions were made
7. **Code generation**: Consider generating readable code for debugging

