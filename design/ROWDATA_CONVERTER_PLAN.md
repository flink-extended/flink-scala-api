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

**Key Points**:
- ✅ Handles null fields correctly via `isNullAt()`
- ✅ Calls type-specific accessor methods (getInt, getLong, getString, etc.)
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

**Implementation Strategy**:
- Use `scala.compiletime.summonInline` to resolve `FieldConverter[T]` per field
- Use `scala.deriving.Mirror` for field extraction
- Generate **type-specific accessor calls** (getInt, getLong, getString, etc.) based on compile-time type info
- Generate unrolled code (not loops) for best performance
- Use `GenericRowData` for RowData creation

**Key Challenges**:
1. **Field Extraction**: Map Mirror labels to actual field names and types
2. **Tuple Unpacking**: Extract individual field types from `MirroredElemTypes`
3. **Type-Specific Accessors**: Generate the right accessor call for each field type
4. **Error Messages**: Make compile errors clear when RowDataConverter can't be derived

**Testing**:
- [ ] Single field case class compiles and works
- [ ] Multi-field case class works
- [ ] Custom converter overrides default
- [ ] Multiple custom converters work
- [ ] Null fields handled
- [ ] Compile errors are clear
- [ ] Type-specific accessors are called (not generic getField)

---

### 2.2 Auto/SemiAuto Derivation Traits
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/auto.scala`
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/semiauto.scala`

Following the existing `TypeInformation` pattern.

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

**Testing**:
- [ ] Extension methods resolve correctly
- [ ] Type inference works
- [ ] Chaining operations

### 3.2 Package Exports
**File**: `modules/flink-common-api/src/main/scala-3/org/apache/flinkx/api/rowdata/package.scala`

**Testing**:
- [ ] All exports resolve
- [ ] No circular dependencies

---

## Phase 4: Real-World Examples

See DETAILED_DESIGN_REFERENCE.md for example code.

---

## Phase 5: Testing Strategy

### Unit Tests
**File**: `modules/flink-common-api/src/test/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterSpec.scala`

### Integration Tests
**File**: `modules/flink-common-api/src/test/scala-3/org/apache/flinkx/api/rowdata/RowDataConverterIntegrationSpec.scala`

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
- [ ] Generate type-specific accessor calls
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

## Key References

- **Flink RowData API**: https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/table/data/RowData.html
- **Type-Specific Accessors**: getInt(), getLong(), getString(), etc.
- **Null Checks**: isNullAt(pos) before accessor calls
- **RowData Creation**: GenericRowData for new instances

---

## Next Steps

1. ✅ Review this implementation plan
2. 🎯 Approve design decisions
3. 🚀 Begin Phase 1 (foundation)
4. 📊 Create JIRA/GitHub issues if needed
5. 📅 Schedule review checkpoints
