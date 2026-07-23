# Interaction with Flink's type system

This Scala API is enforcing usage of Flink's `TypeInformation` objects by requiring them to be implicitly available in the scope. It plays well with the derivation macro generating TypeInformations for Scala ADTs.

However, this project cannot enforce TypeInformation usage in the Flink Java API where there is other ways to provide information on types to Flink, notably using `Class`, for exemple:
- `TypeInformation.of(Class<T>)`
- `StateDescriptor` and subclasses: constructors with a `Class<T>` param
- `TypeHint`

Usage of this code may lead to silently fallback to Kryo.

From Flink 1.19, a check is done to detect this misusage. To disable it, see [Disable fail-fast on Scala type resolution with Class feature flag](feature-flags.md#disable-fail-fast-on-scala-type-resolution-with-class).

> [!WARNING]  
> Official `flink-scala` deprecated dependency contains Scala-specialized Kryo serializers. If this dependency is removed from the classpath (see [Supported Flink versions](getting-started.md#supported-flink-versions)), usage of Kryo with Scala classes leads to erroneous re-instantiations of `object` and `case object` singletons.
> 
> We recommend to test your application with Kryo explicitly disabled (Flink property `pipeline.generic-types: false`).

## Flink ADT

To derive a TypeInformation for a case class or sealed trait, you can do:

```scala mdoc:reset-object
import org.apache.flinkx.api.semiauto._
import org.apache.flink.api.common.typeinfo.TypeInformation

sealed trait Event extends Product with Serializable

object Event {
  final case class Click(id: String) extends Event
  final case class Purchase(price: Double) extends Event

  implicit val eventTypeInfo: TypeInformation[Event] = deriveTypeInformation
}
```

Be careful with a wildcard import of import `org.apache.flink.api.scala._`: it has a `createTypeInformation` implicit function, which may happily generate you a kryo-based serializer in a place you never expected. So in a case if you want to do this type of wildcard import, make sure that you explicitly called `deriveTypeInformation` for all the sealed traits in the current scope.

### Auto and semi-auto derivation

This library provides two approaches for deriving TypeInformation: **automatic** (`auto`) and **semi-automatic** (`semiauto`).

#### Automatic derivation

Import `org.apache.flinkx.api.auto._` when you want TypeInformation to be derived automatically:

```scala mdoc:reset-object
import org.apache.flinkx.api._
import org.apache.flinkx.api.auto._ // Automatic derivation

case class User(id: String, age: Int)

val env = StreamExecutionEnvironment.getExecutionEnvironment

// TypeInformation is derived automatically
env.fromElements(User("alice", 30), User("bob", 25))
```

Auto TypeInformation derivation is called implicitly whenever needed. This automatic behavior is easier and more convenient when you don't need a fine control over derivation.

#### Semi-automatic derivation

Import `org.apache.flinkx.api.semiauto._` when you want explicit control over TypeInformation derivation:

```scala mdoc:reset-object
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api._
import org.apache.flinkx.api.semiauto._ // Manual derivation

case class User(id: String, age: Int)

object User {
  // Explicitly derive and cache TypeInformation
  implicit val userInfo: TypeInformation[User] = deriveTypeInformation[User]
}

val env = StreamExecutionEnvironment.getExecutionEnvironment

// Uses pre-derived TypeInformation found in User companion object
env.fromElements(User("alice", 30), User("bob", 25))
```

A good practice is to declare the type-information as implicit val in the companion object of the case class, it's derived once, cached, and will be available in the implicit context wherever the case class is used.

Benefits of `semiauto`:
- Control: Choose exactly which types have TypeInformation
- Better compile times: TypeInformation is derived once and cached

### Null value handling

A case class can be null, the case class serializer natively handles the null case.

A case class field can also be null, either:
- the serializer of this field natively handles its nullability.
- the field must be annotated with `@nullable` in order to be wrapped in Flink's `NullableSerializer`.

In any cases, it can be a good hint to use `@nullable` annotation to indicate when fields are meant to be nullable.

```scala mdoc:reset-object
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.serializer.nullable
import org.apache.flink.api.common.typeinfo.TypeInformation

case class Click(id: String, clickEvent: ClickEvent)

case class ClickEvent(
    @nullable history: Array[String], // @nullable allows to handle null array
    @nullable id: String) // Effectless here as null strings are natively handled

Click("id1", null) // A case class can be null
Click("id2", ClickEvent(null, null)) // Valid thanks to @nullable
```

## Java types

Built-in serializers are for Scala language abstractions and won't derive `TypeInformation` for Java classes (as they don't extend the `scala.Product` type). But you can always fall back to Flink's own POJO serializer in this way, so just make it implicit so this API can pick it up:

```scala mdoc:reset-object
import java.time.LocalDate
import org.apache.flink.api.common.typeinfo.TypeInformation

implicit val localDateTypeInfo: TypeInformation[LocalDate] = TypeInformation.of(classOf[LocalDate])
```

## Type mapping

Sometimes built-in serializers may spot a type (usually a Java one), which cannot be directly serialized as a case class, like this 
example:

```scala mdoc:reset-object
class WrappedString {
  private var internal: String = ""

  override def equals(obj: Any): Boolean = 
    obj match {
      case s: WrappedString => s.get == internal
      case _                => false
    }
  
  def get: String = internal
  def put(value: String) =
    internal = value 
}   
```

You can write a pair of explicit `TypeInformation[WrappedString]` and `Serializer[WrappedString]`, but it's extremely verbose,
and the class itself can be 1-to-1 mapped to a regular `String`. This library has a mechanism of type mappers to delegate serialization
of non-serializable types to existing serializers. For example:

```scala mdoc
import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.auto._

class WrappedMapper extends TypeMapper[WrappedString, String] {
  override def map(a: WrappedString): String = a.get

  override def contramap(b: String): WrappedString = {
    val str = new WrappedString
    str.put(b)
    str
  }  
}

implicit val mapper: TypeMapper[WrappedString, String] = new WrappedMapper()
// will treat WrappedString with String typeinfo:
implicit val ti: TypeInformation[WrappedString] = mappedTypeInfo[WrappedString, String]
```

When there is a `TypeMapper[A, B]` in the scope to convert `A` to `B` and back, and type `B` has `TypeInformation[B]` available 
in the scope also, then this library will use a delegated existing typeinfo for `B` when it will spot type `A`.

Warning: on Scala 3, the TypeMapper should not be made anonymous. This example won't work, as anonymous implicit classes in 
Scala 3 are private, and Flink cannot instantiate it on restore without JVM 17 incompatible reflection hacks:

```scala mdoc:reset-object
import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper

class WrappedString {
  private var internal: String = ""

  override def equals(obj: Any): Boolean = 
    obj match {
      case s: WrappedString => s.get == internal
      case _                => false
    }

  def get: String = internal
  def put(value: String) =
    internal = value
}  
  
class WrappedMapper extends TypeMapper[WrappedString, String] {
  override def map(a: WrappedString): String = a.get

  override def contramap(b: String): WrappedString = {
    val str = new WrappedString
    str.put(b)
    str  
  }
}
// anonymous class, will fail on runtime on scala 3
implicit val mapper2: TypeMapper[WrappedString, String] = new TypeMapper[WrappedString, String] {
  override def map(a: WrappedString): String = a.get

  override def contramap(b: String): WrappedString = {
    val str = new WrappedString
    str.put(b)
    str  
  }
}  
```

## Ordering

`SortedSet` requires a type-information for its elements and also for the ordering of the elements. Type-information of default orderings are not implicitly available in the context because we cannot make the assumption the user wants to use the natural ordering or a custom one.

Type-information of default ordering are available in `org.apache.flinkx.api.serializer.OrderingTypeInfo` and can be used as follows:
```scala mdoc:reset-object
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api._
import org.apache.flinkx.api.serializer.OrderingTypeInfo
import org.apache.flinkx.api.auto._
import scala.collection.immutable.SortedSet

case class Foo(bars: SortedSet[String])

object Foo {
  implicit val fooInfo: TypeInformation[Foo] = {
    // type-information for Ordering need to be explicitly put in the context
    implicit val orderingStringInfo: TypeInformation[Ordering[String]] =
      OrderingTypeInfo.DefaultStringOrderingInfo
    deriveTypeInformation
  }
}
```

It's also possible to derive the type-information of a custom ordering if it's an ADT:
```scala mdoc:reset-object
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api._
import org.apache.flinkx.api.serializer.OrderingTypeInfo
import org.apache.flinkx.api.auto._
import scala.collection.immutable.SortedSet

case class Bar(a: Int, b: String)

case object BarOrdering extends Ordering[Bar] {
  override def compare(x: Bar, y: Bar): Int = x.a.compare(y.a)
}

case class Foo(bar: SortedSet[Bar])

object Foo {
  implicit val fooInfo: TypeInformation[Foo] = {
    // Derive the type-information of custom Bar ordering
    implicit val barOrderingInfo: TypeInformation[Ordering[Bar]] =
      OrderingTypeInfo.deriveOrdering[BarOrdering.type, Bar]
    deriveTypeInformation
  }
}
```

## Schema evolution

### ADT
For the child case classes being part of ADT, the serializers use a Flink's `CaseClassSerializer`, so all the compatibility rules
are the same as for normal case classes.

For the sealed trait membership itself, this library uses own serialization format with the following rules:
* you cannot reorder trait members, as wire format depends on the compile-time index of each member
* you can add new members at the end of the list
* you cannot remove ADT members
* you cannot replace ADT members

### Case Class Changes

On a case class level, this library supports new field addition(s) with default value(s). This allows to restore a Flink job from a savepoint created using previous case class schema.
For example:

1. A Flink job was stopped with a savepoint using below case class schema:
```scala
case class Click(id: String, inFileClicks: List[ClickEvent])
```
2. Now the Click case class is changed to:
```scala
case class Click(id: String, inFileClicks: List[ClickEvent], 
    fieldInFile: String = "test1",
    fieldNotInFile: String = "test2")   
```
3. Launch the same job with new case class schema version from the last savepoint. Job restore should work successfully.

## Compatibility

This project uses a separate set of serializers for collections, instead of Flink's own TraversableSerializer. So probably you
may have issues while migrating state snapshots from TraversableSerializer to this project serializers.
