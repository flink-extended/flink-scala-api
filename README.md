# Scala 2.13/3.x API for Apache Flink

[![CI Status](https://github.com/flink-extended/flink-scala-api/workflows/CI/badge.svg)](https://github.com/flinkextended/flink-scala-api/actions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.flinkextended/flink-scala-api_2.13/badge.svg?style=plastic)](https://maven-badges.herokuapp.com/maven-central/org.flinkextended/flink-scala-api_2.13)
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
```scala mdoc:reset-object
import org.apache.flinkx.api.serializers._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation

// state using flink-scala-api
val eventStateDescriptor = new ValueStateDescriptor[Option[String]]("event",
  implicitly[TypeInformation[Option[String]]])
```

## Usage 

`flink-scala-api` is released to Maven-central for 2.13 and 3. For SBT, add this snippet to `build.sbt`:
```scala
libraryDependencies += "org.flinkextended" %% "flink-scala-api" % "1.18.1_1.2.1"
```

## For Ammonite

```scala
import $ivy.`org.flinkextended::flink-scala-api:1.18.1_1.2.1`
// you might need flink-client too in order to run in the REPL
import $ivy.`org.apache.flink:flink-clients:1.18.1`
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

## SBT Project Template

If you want to create new project easily check this __Giter8 template__ out: [novakov-alexey/flink-scala-api.g8](https://github.com/novakov-alexey/flink-scala-api.g8)

## Supported Flink versions

- `flink-scala-api` version consists of Flink version plus Scala API version, for example 1.18.1_1.1.6
- First three numbers correspond to the Flink Version, for example 1.18.1
- Three more numbers is this project version, for example 1.1.6. You should just use the latest available Scala API project version in in your project dependency configuration. 
- Three major Flink versions are supported. See supported version in the local [release.sh](release.sh) file.

We suggest to remove the official `flink-scala` and `flink-streaming-scala` dependencies altogether to simplify the migration and do not to mix two flavors of API in the same project. But it's technically possible and not required.

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
* has no silent fallback to Kryo: it will just fail the compilation in a case when serializer cannot be made
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
[POJO serializer](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/fault-tolerance/serialization/types_serialization/#rules-for-pojo-types),
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

## Flink ADT

To derive a TypeInformation for a sealed trait, you can do:

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

### Schema evolution

#### ADT
For the child case classes being part of ADT, the serializers use a Flink's `ScalaCaseClassSerializer`, so all the compatibility rules
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

### Disable Class Arity Usage

In the [1.1.5 release](https://github.com/flink-extended/flink-scala-api/releases/tag/v1.18.1_1.1.5) the Case Class serialization process also [stores case class arity](https://github.com/flink-extended/flink-scala-api/pull/98/files#diff-e896c210d6a754cb3afb462aea34cca08f090330f6f3c663a64dfb5584fc3727R106) number to
a savepoint. This was introduced to support Case Class schema evolution and allow to add new
class fields with default values. However, unfortunatelly this is the breaking change to the Flink job state restore process. Flink job will fail, if
a savepoint used for the job restore was created by 1.1.4 or earlier releases.

In order migrate to the 1.1.5 release version, one can use specially added environment variable:
`DISABLE_CASE_CLASS_ARITY_USAGE`.

To disable new savepoint format and be able to restore a Flink job with a savepoint created before 1.1.5 release set the variable to `true`. 

Example: `DISABLE_CASE_CLASS_ARITY_USAGE = true`

To enbale new serialization logic set this variable to `false` or simply do not define this envrionment vairable.

Example: `DISABLE_CASE_CLASS_ARITY_USAGE = false`

P.S. this flag can be deprecated in future when most of the users migrate to the latest library version.

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
RELEASE_PUBLISH=true sbt "; project scala-api; release with-defaults"
```

## License

This project is using parts of the Apache Flink codebase, so the whole project
is licensed under an [Apache 2.0](LICENSE.md) software license.
