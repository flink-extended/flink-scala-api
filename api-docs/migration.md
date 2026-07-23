# Migration

`flink-scala-api` uses a different package name for all api-related classes like `DataStream`, so you can do
gradual migration of a big project and use both upstream and this versions of scala API in the same project. 

## API

The actual migration should be straightforward and simple, replace old import to the new ones:
```scala
// original api import
import org.apache.flink.streaming.api.scala._
```
```scala mdoc
// flink-scala-api imports
import org.apache.flinkx.api._
import org.apache.flinkx.api.auto._
```

## State

Ensure to replace state descriptor constructors using `Class[T]` param with constructors using `TypeInformation[T]` or `TypeSerializer[T]` param:
```scala
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation

// state using Kryo
val eventStateDescriptor = new ValueStateDescriptor[Option[String]]("event",
  classOf[Option[String]])
```
Although `flink-scala-api` does not fallback to Kryo silently for Scala types, above code example will eventually use Kryo. This happens when official flink-scala-api (deprecated) is already disabled, but this Flink Scala API is not yet properly used: no serializers are used/imported from `org.apache.flinkx.api.auto._`.
Below code example is the right way to use Scala serializers coming from this Scala API: it completely prevents Kryo from being used. Even if `implicitly` cannot find an appropriate TypeInformation instance for your type `T`, it will fail in compile time.

```scala mdoc:reset-object
import org.apache.flinkx.api.auto._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation

// state using flink-scala-api
val eventStateDescriptor = new ValueStateDescriptor[Option[String]]("event",
  implicitly[TypeInformation[Option[String]]])
```
- See more on serialization with POJO serializer here: [Using a POJO-only Flink serialization framework](differences.md#using-a-pojo-only-flink-serialization-framework)
- See more on Java types serilization here: [Java types](type-system.md#java-types)
