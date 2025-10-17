# Scala 2.13/3.x API for Apache Flink

[![CI Status](https://github.com/flink-extended/flink-scala-api/workflows/CI/badge.svg)](https://github.com/flinkextended/flink-scala-api/actions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.flinkextended/flink-scala-api-1_2.13/badge.svg?style=plastic)](https://maven-badges.herokuapp.com/maven-central/org.flinkextended/flink-scala-api-1_2.13)
[![License: Apache 2](https://img.shields.io/badge/License-Apache2-green.svg)](https://opensource.org/licenses/Apache-2.0)
![Last commit](https://img.shields.io/github/last-commit/flink-extended/flink-scala-api)
![Last release](https://img.shields.io/github/release/flink-extended/flink-scala-api)

This project is a community-maintained fork of official Apache Flink Scala API, cross-built for scala 2.13 and 3.x.

## Migration 

`flink-scala-api` uses a different package name for all api-related classes like `DataStream`, so you can do
gradual migration of a big project and use both upstream and this versions of scala API in the same project. 

### API

The actual migration should be straightforward and simple, replace old import to the new ones:
```scala
// original api import
import org.apache.flink.streaming.api.scala._
```
```scala mdoc
// flink-scala-api imports
import org.apache.flinkx.api._
import org.apache.flinkx.api.serializers._
```

### State

Ensure to replace state descriptor constructors using `Class[T]` param with constructors using `TypeInformation[T]` or `TypeSerializer[T]` param:
```scala mdoc
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation

// state using Kryo
val eventStateDescriptor = new ValueStateDescriptor[Option[String]]("event",
  classOf[Option[String]])
```
Although `flink-scala-api` does not fallback to Kryo silently for Scala types, above code example will eventually use Kryo. This happens when official flink-scala-api (deprecated) is already disabled, but this Flink Scala API is not yet properly used: no serializers are used/imported from `org.apache.flinkx.api.serializers._`.
Below code example is the right way to use Scala serializers coming from this Scala API: it completely prevents Kryo from being used. Even if `implicitly` cannot find an appropriate TypeInformation instance for your type `T`, it will fail in compile time.

```scala mdoc:reset-object
import org.apache.flinkx.api.serializers._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation

// state using flink-scala-api
val eventStateDescriptor = new ValueStateDescriptor[Option[String]]("event",
  implicitly[TypeInformation[Option[String]]])
```
- See more on serialization with POJO serializer here: [using-a-pojo-only-flink-serialization-framework](#using-a-pojo-only-flink-serialization-framework)
- See more on Java types serilization here: [java-types](#java-types)

## Usage 

`flink-scala-api` is released to Maven-central for 2.13 and 3. For SBT, add this snippet to `build.sbt`:
```scala
libraryDependencies += "org.flinkextended" %% "flink-scala-api-1" % "1.2.6"
// or for Flink 2
"org.flinkextended" %% "flink-scala-api-2" % "1.2.6"
```

## For Ammonite

```scala
import $ivy.`org.flinkextended::flink-scala-api-1:1.2.6`
// you might need flink-client too in order to run in the REPL
import $ivy.`org.apache.flink:flink-clients:1.20.1`
```

## For Scala 2.12

If you want first to migrate to org.flinkextended:flink-scala-api staying on Scala 2.12, you can use the last build for Scala 2.12:

```scala
libraryDependencies += "org.flinkextended" %% "flink-scala-api" % "1.18.1_1.2.0"
// or
"org.flinkextended" %% "flink-scala-api" % "1.19.1_1.2.0"
// or
"org.flinkextended" %% "flink-scala-api" % "1.20.0_1.2.0"
```

Build for Scala 2.12 is no longer published.

## Dependencies management
When including the Scala API in a fat JAR or adding it to the __flink/lib__ folder, users should ensure that the appropriate Scala API dependencies are included
into flink classpath.
In the case of Scala versions, these dependencies should be:

### For Scala 2.13

- [scala-reflect](https://mvnrepository.com/artifact/org.scala-lang/scala-reflect)
- [magnolia](https://mvnrepository.com/artifact/com.softwaremill.magnolia1_2/magnolia_2.13)

### For Scala 3
- [magnolia](https://mvnrepository.com/artifact/com.softwaremill.magnolia1_3/magnolia)

## SBT Project Template

If you want to create new project easily check this __Giter8 template__ out: [novakov-alexey/flink-scala-api.g8](https://github.com/novakov-alexey/flink-scala-api.g8)

## Supported Flink versions

Three latest of Apache Flink 1.x and one Flink 2.y versions are supported. 
See excact supported versions in the local [ci.yml](.github/workflows/ci.yml#L16) file.

We suggest to remove the official `flink-scala` and `flink-streaming-scala` deprecated dependencies altogether to simplify the migration and do not to mix two flavors of API in the same project. `flink-scala` dependency is embedding Scala version 2.12.7:
- If you keep them, in order to use the Scala version of your choice, remove `scala` package from `classloader.parent-first-patterns.default` Flink's configuration property:
```diff
- classloader.parent-first-patterns.default: java.;scala.;org.apache.flink.;com.esotericsoftware.kryo;org.apache.hadoop.;javax.annotation.;org.xml;javax.xml;org.apache.xerces;org.w3c;org.rocksdb.;org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback
+ classloader.parent-first-patterns.default: java.;org.apache.flink.;com.esotericsoftware.kryo;org.apache.hadoop.;javax.annotation.;org.xml;javax.xml;org.apache.xerces;org.w3c;org.rocksdb.;org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback
```
- If you choose to remove them, we recommend to test your application with Kryo explicitly disabled (Flink property `pipeline.generic-types: false`), see details in [Interaction with Flink's type system](#interaction-with-flinks-type-system).

## Examples

There is a wide range of [code examples](https://github.com/flink-extended/flink-scala-api/tree/master/modules/examples) to introduce you to flink-scala-api, both using Scala scripts and multimodule applications. These examples include:

- Flink jobs built using Scala 3 with Ammonite and Scala CLI;
- Streaming job using Google Pub/Sub and JSON serialiser;
- A complete application for fraud detection;
- Examples using Datastream and Table APIs;
- Simple job developed interactively via Jupyter notebooks;
- Word count reading text from a web socket;
- Example usage of DataGen connector and Kafka sink;
- And more;

## Differences with the Official Flink Scala API

### New [magnolia](https://github.com/softwaremill/magnolia)-based serialization framework

Official Flink's serialization framework has two important drawbacks complicating the upgrade to Scala 2.13+:
* it used a complicated `TypeInformation` derivation macro, which required a complete rewrite to work on Scala 3.
* for serializing a `Traversable[_]` it serialized an actual scala code of the corresponding `CanBuildFrom[_]` builder,
which was compiled and executed on deserialization. There is no more `CanBuildFrom[_]` on Scala 2.13+, so there is
no easy way of migration

This project comes with special functionality for Scala ADTs to derive serializers for all 
types with the following perks:

* can support ADTs (Algebraic data types, sealed trait hierarchies)
* correctly handles `case object` 
* can be extended with custom serializers even for deeply-nested types, as it uses implicitly available serializers
  in the current scope
* `TypeInformation` derivation macro has no silent fallback to Kryo: it will just fail the compilation in a case when serializer cannot be made
* reuses all the low-level serialization code from Flink for basic Java and Scala types

Scala serializers are based on a prototype of Magnolia-based serializer framework for Apache Flink, with
more Scala-specific TypeSerializer & TypeInformation derivation support.

There are some drawbacks when using this functionality:
* Savepoints written using Flink's official serialization API are not compatible, so you need to re-bootstrap your job
from scratch.
* As serializer derivation happens in a compile-time and uses zero runtime reflection, for deeply-nested rich case
classes the compile times are quite high.


### Using a POJO-only Flink serialization framework

If you don't want to use built-in Scala serializers for some reasons, you can always fall back to the Flink
[POJO serializer](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/fault-tolerance/serialization/types_serialization/#rules-for-pojo-types),
explicitly calling it:
```scala mdoc:reset-object
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api._

implicit val intInfo: TypeInformation[Int] = TypeInformation.of(classOf[Int]) // explicit usage of the POJO serializer

val env = StreamExecutionEnvironment.getExecutionEnvironment
env
  .fromElements(1, 2, 3)
  .map(x => x + 1)
```

With this approach:
* savepoint compatibility between this and official Flink API
* slower serialization type due to frequent Kryo fallback
* larger savepoint size (again, due to Kryo)

### Closure cleaner from Spark 3.x

Flink historically used quite an old forked version of the ClosureCleaner for scala lambdas, which has some minor
compatibility issues with Java 17 and Scala 2.13+. This project uses a more recent version, hopefully with less
compatibility issues.

### No Legacy DataSet API

Sorry, but it's already deprecated and as a community project we have no resources to support it. If you need it,
PRs are welcome.

## Interaction with Flink's type system

This Scala API is enforcing usage of Flink's `TypeInformation` objects by requiring them to be implicitly available in the scope. It plays well with the derivation macro generating TypeInformations for Scala ADTs.

However, this project cannot enforce TypeInformation usage in the Flink Java API where there is other ways to provide information on types to Flink, notably using `Class`, for exemple:
- `TypeInformation.of(Class<T>)`
- `StateDescriptor` and subclasses: constructors with a `Class<T>` param
- `TypeHint`

Usage of this code may lead to silently fallback to Kryo.

From Flink 1.19, a check is done to detect this misusage. To disable it, see [Disable fail-fast on Scala type resolution with Class feature flag](#disable-fail-fast-on-scala-type-resolution-with-class).

> [!WARNING]  
> Official `flink-scala` deprecated dependency contains Scala-specialized Kryo serializers. If this dependency is removed from the classpath (see [Supported Flink versions](#supported-flink-versions)), usage of Kryo with Scala classes leads to erroneous re-instantiations of `object` and `case object` singletons.
> 
> We recommend to test your application with Kryo explicitly disabled (Flink property `pipeline.generic-types: false`).

### Flink ADT

To derive a TypeInformation for a case class or sealed trait, you can do:

```scala mdoc:reset-object
import org.apache.flinkx.api.serializers._
import org.apache.flink.api.common.typeinfo.TypeInformation

sealed trait Event extends Product with Serializable

object Event {
  final case class Click(id: String) extends Event
  final case class Purchase(price: Double) extends Event

  implicit val eventTypeInfo: TypeInformation[Event] = deriveTypeInformation
}  
```

Be careful with a wildcard import of import `org.apache.flink.api.scala._`: it has a `createTypeInformation` implicit function, which may happily generate you a kryo-based serializer in a place you never expected. So in a case if you want to do this type of wildcard import, make sure that you explicitly called `deriveTypeInformation` for all the sealed traits in the current scope.

#### Null value handling

A case class can be null, the case class serializer natively handles the null case.

A case class field can also be null, either:
- the serializer of this field natively handles its nullability.
- the field must be annotated with `@nullable` in order to be wrapped in Flink's `NullableSerializer`.

In any cases, it can be a good hint to use `@nullable` annotation to indicate when fields are meant to be nullable.

```scala mdoc:reset-object
import org.apache.flinkx.api.serializers._
import org.apache.flinkx.api.serializer.nullable
import org.apache.flink.api.common.typeinfo.TypeInformation

case class Click(id: String, clickEvent: ClickEvent)

case class ClickEvent(
    @nullable history: Array[String], // @nullable allows to handle null array
    @nullable id: String) // Effectless here as null strings are natively handled

Click("id1", null) // A case class can be null
Click("id2", ClickEvent(null, null)) // Valid thanks to @nullable
```

### Java types

Built-in serializers are for Scala language abstractions and won't derive `TypeInformation` for Java classes (as they don't extend the `scala.Product` type). But you can always fall back to Flink's own POJO serializer in this way, so just make it implicit so this API can pick it up:

```scala mdoc:reset-object
import java.time.LocalDate
import org.apache.flink.api.common.typeinfo.TypeInformation

implicit val localDateTypeInfo: TypeInformation[LocalDate] = TypeInformation.of(classOf[LocalDate])
```

### Type mapping

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
import org.apache.flinkx.api.serializers._

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

### Ordering

`SortedSet` requires a type-information for its elements and also for the ordering of the elements. Type-information of default orderings are not implicitly available in the context because we cannot make the assumption the user wants to use the natural ordering or a custom one.

Type-information of default ordering are available in `org.apache.flinkx.api.serializer.OrderingTypeInfo` and can be used as follows:
```scala mdoc:reset-object
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api._
import org.apache.flinkx.api.serializer.OrderingTypeInfo
import org.apache.flinkx.api.serializers._
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
import org.apache.flinkx.api.serializers._
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

### Schema evolution

#### ADT
For the child case classes being part of ADT, the serializers use a Flink's `CaseClassSerializer`, so all the compatibility rules
are the same as for normal case classes.

For the sealed trait membership itself, this library uses own serialization format with the following rules:
* you cannot reorder trait members, as wire format depends on the compile-time index of each member
* you can add new members at the end of the list
* you cannot remove ADT members
* you cannot replace ADT members

#### Case Class Changes

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

### Compatibility

This project uses a separate set of serializers for collections, instead of Flink's own TraversableSerializer. So probably you
may have issues while migrating state snapshots from TraversableSerializer to this project serializers.

## Scala 3

Scala 3 support is highly experimental and not well-tested in production. Good thing is that most of the issues are compile-time, 
so quite easy to reproduce. If you have issues with this library not deriving `TypeInformation[T]` for the `T` you want, submit a bug report!

## Compile times

They may be quite bad for rich nested case classes due to compile-time serializer derivation. 
Derivation happens each time `flink-scala-api` needs an instance of the `TypeInformation[T]` implicit/type class:
```scala mdoc:reset-object
import org.apache.flinkx.api._
import org.apache.flinkx.api.serializers._

case class Foo(x: Int) {
  def inc(a: Int) = copy(x = x + a)
}  

val env = StreamExecutionEnvironment.getExecutionEnvironment
env
  .fromElements(Foo(1), Foo(2), Foo(3))
  .map(x => x.inc(1)) // here the TypeInformation[Foo] is generated
  .map(x => x.inc(2)) // generated one more time again
```

If you're using the same instances of data structures in multiple jobs (or in multiple tests), consider caching the
derived serializer in a separate compile unit and just importing it when needed:

```scala mdoc:reset-object
import org.apache.flinkx.api._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.serializers._

// file FooTypeInfo.scala
object FooTypeInfo {
  lazy val fooTypeInfo: TypeInformation[Foo] = deriveTypeInformation[Foo]
}
// file SomeJob.scala
case class Foo(x: Int) {
  def inc(a: Int) = copy(x = x + a)
}

import FooTypeInfo._

val env = StreamExecutionEnvironment.getExecutionEnvironment
env
  .fromElements(Foo(1),Foo(2),Foo(3))
  .map(x => x.inc(1)) // taken as an implicit
  .map(x => x.inc(2)) // again, no re-derivation
```

## Feature Flags

### Disable fail-fast on Scala type resolution with Class

From [1.2.3 release](https://github.com/flink-extended/flink-scala-api/releases/tag/v1.20.0_1.2.3), a check is done to prevent misusage of Scala type resolution with `Class` which may lead to silently fallback to generic Kryo serializers.

You can disable this check with the `DISABLE_FAIL_FAST_ON_SCALA_TYPE_RESOLUTION_WITH_CLASS` environment variable set to `true`.

## Release

Create SBT file at ~/.sbt/1.0/sonatype.sbt with the following content:

```bash
credentials += Credentials("Sonatype Nexus Repository Manager",
        "s01.oss.sonatype.org",
        "<access token: user name>",
        "<access token: password>")
```

replace values with your access token user name and password.

Release new version:

```bash
sh release.sh
```

Increment to next SNAPSHOT version and push to Git server:

```bash
RELEASE_PUBLISH=true sbt "; release with-defaults"
```

## License

This project is using parts of the Apache Flink codebase, so the whole project
is licensed under an [Apache 2.0](LICENSE.md) software license.
