# Scala 2.12/2.13/3.x API for Apache Flink

[![CI Status](https://github.com/flink-extended/flink-scala-api/workflows/CI/badge.svg)](https://github.com/flink-extended/flink-scala-api/actions)
[![License: Apache 2](https://img.shields.io/badge/License-Apache2-green.svg)](https://opensource.org/licenses/Apache-2.0)
![Last commit](https://img.shields.io/github/last-commit/flink-extended/flink-scala-api)
![Last release](https://img.shields.io/github/release/flink-extended/flink-scala-api)

This project is a community-maintained fork of official Apache Flink Scala API, cross-built for scala 2.12, 2.13 and 3.x.

## Differences

### New [magnolia](https://github.com/softwaremill/magnolia)-based serialization framework

Official Flink's serialization framework has two important drawbacks complicating the upgrade to Scala 2.13+:
* it used a complicated `TypeInformation` derivation macro, which required a complete rewrite to work on Scala 3.
* for serializing a `Traversable[_]` it serialized an actual scala code of the corresponding `CanBuildFrom[_]` builder,
which was compiled and executed on deserialization. There is no more `CanBuildFrom[_]` on Scala 2.13+, so there is
no easy way of migration

This project relies on the [Flink-ADT](https://github.com/findify/flink-adt) library to derive serializers for all 
types with the following perks:
* ADT support: so your `sealed trait` members won't fall back to extremely slow Kryo serializer
* case objects: no more problems with `None`
* uses implicits (and typeclasses in Scala 3) to customize the serialization

But there are some drawbacks:
* Savepoints written using Flink's official serialization API are not compatible, so you need to re-bootstrap your job
from scratch.
* As serializer derivation happens in a compile-time and uses zero runtime reflection, for deeply-nested rich case
classes the compile times are quite high.

See [Flink-ADT](https://github.com/findify/flink-adt) readme for more details.

### Using a POJO-only Flink serialization framework

If you don't want to use a `Flink-ADT` for serialization for some reasons, you can always fall back to a flink's
[POJO serializer](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/fault-tolerance/serialization/types_serialization/#rules-for-pojo-types),
explicitly calling it:
```scala
val env = StreamingExecutionEnvironment.createLocalEnvironment()
env
  .fromCollection(1,2,3)
  .map(x => x + 1)(TypeInformation.of[Int]) // explicit call
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

## Migration 

`flink-scala-api` uses a different package name for all api-related classes like `DataStream`, so you can do
gradual migration of a big project and use both upstream and this versions of scala API in the same project. 

The actual migration should be straightforward and simple, replace old import to the new ones:
```scala
// original api import
import org.apache.flink.streaming.api.scala._

// flink-scala-api imports
import org.apache.flink.api._
import io.findify.flinkadt.api._
```

## Usage 

`flink-scala-api` is released to Maven-central for 2.12, 2.13 and 3. For SBT, add this snippet to `build.sbt`:
```scala
libraryDependencies += "org.flinkextended" %% "flink-scala-api" % "1.16.1.2"
```

Flink version notes:

- `flink-scala-api` contains Flink version in its onw version to help users to find right version for their Flink based project
- First three numbers correspond to Flink Version, for example 1.16.1 
- Last forth digit is an internal project build version. You should just use the last build number in your dependency configuration. 

We suggest to remove the official `flink-scala` and `flink-streaming-scala` dependencies altogether to simplify the migration and do not to mix two flavors of API in the same project. But it's technically possible and not required.

## Scala 3

Scala 3 support is highly experimental and not well-tested in production. Good thing is that most of the issues are compile-time, 
so quite easy to reproduce. If you have issues with `flink-adt` not deriving `TypeInformation[T]` for the `T` you want, 
submit a bug report!

## Compile times

They may be quite bad for rich nested case classes due to compile-time serializer derivation. 
Derivation happens each time `flink-scala-api` needs an instance of the `TypeInformation[T]` implicit/type class:
```scala
case class Foo(x: Int) {
  def inc(a: Int) = copy(x = x + a)
}

val env = StreamingExecutionEnvironment.createLocalEnvironment()
env
  .fromCollection(List(Foo(1),Foo(2),Foo(3)))
  .map(x => x.inc(1)) // here the TypeInformation[Foo] is generated
  .map(x => x.inc(2)) // generated one more time again
```

If you're using the same instances of data structures in multiple jobs (or in multiple tests), consider caching the
derived serializer in a separate compile unit and just importing it when needed:

```scala
// file FooTypeInfo.scala
object FooTypeInfo {
  lazy val fooTypeInfo: TypeInformation[Foo] = deriveTypeInformation[Foo]
}

// file SomeJob.scala
case class Foo(x: Int) {
  def inc(a: Int) = copy(x = x + a)
}

import FooTypeInfo._

val env = StreamingExecutionEnvironment.createLocalEnvironment()
env
  .fromCollection(List(Foo(1),Foo(2),Foo(3)))
  .map(x => x.inc(1)) // taken as an implicit
  .map(x => x.inc(2)) // again, no re-derivation

```

## Release

Define two environment variables before starting SBT shell: 

```bash
export SONATYPE_USERNAME=<your user name for Sonatype>
export SONATYPE_PASSWORD=<your password for Sonatype> 
```

Release new version:

```bash
RELEASE_VERSION_BUMP=true sbt test 'release with-defaults'
```

Increment to next SNAPSHOT version and push to Git server:

```bash
RELEASE_PUBLISH=true sbt 'release with-defaults'
```

## License

This project is using parts of the Apache Flink codebase, so the whole project
is licensed under an [Apache 2.0](LICENSE.md) software license.