# Differences with the Official Flink Scala API

## New [magnolia](https://github.com/softwaremill/magnolia)-based serialization framework

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


## Using a POJO-only Flink serialization framework

If you don't want to use built-in Scala serializers for some reason, you can always fall back to the Flink
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

## Closure cleaner from Spark 3.x

Flink historically used quite an old forked version of the ClosureCleaner for scala lambdas, which has some minor
compatibility issues with Java 17 and Scala 2.13+. This project uses a more recent version, hopefully with less
compatibility issues.

## No Legacy DataSet API

Sorry, but it's already deprecated and as a community project we have no resources to support it. If you need it,
PRs are welcome.
