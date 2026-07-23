# Compile times

Automatic TypeInformation derivation may be quite bad for compilation times of rich nested case classes. 
Automatic derivation happens each time `flink-scala-api` needs an instance of the `TypeInformation[T]` implicit type class:
```scala mdoc:reset-object
import org.apache.flinkx.api._
import org.apache.flinkx.api.auto._

case class Foo(x: Int) {
  def inc(a: Int) = copy(x = x + a)
}  

val env = StreamExecutionEnvironment.getExecutionEnvironment
env
  .fromElements(Foo(1), Foo(2), Foo(3))
  .map(x => x.inc(1)) // here the TypeInformation[Foo] is generated
  .map(x => x.inc(2)) // generated one more time again
```

## Caching derivations

If you're using the same instances of data structures in multiple operations (or in multiple tests), consider caching
the derived serializer in a separate compile unit and just importing it when needed:

```scala mdoc:reset-object
import org.apache.flinkx.api._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.auto._

// file FooTypeInfo.scala
object FooTypeInfo {
  lazy implicit val fooTypeInfo: TypeInformation[Foo] = deriveTypeInformation[Foo]
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

Explicit import of `FooTypeInfo` is important to give `fooTypeInfo` cached val a higher priority in implicit context than `deriveTypeInformation`. Declaring cached val in the companion object of `Foo` won't give it a high enough priority.

## Using semi-automatic derivation

Another possibility to improve compilation times is to cache semi-automatic derivation in the companion object of the case class or sealed trait:

```scala mdoc:reset-object
import org.apache.flinkx.api._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.semiauto._

// file SomeJob.scala
case class Foo(x: Int) {
  def inc(a: Int) = copy(x = x + a)
}

object Foo {
  lazy implicit val fooTypeInfo: TypeInformation[Foo] = deriveTypeInformation[Foo]
}

val env = StreamExecutionEnvironment.getExecutionEnvironment
env
  .fromElements(Foo(1),Foo(2),Foo(3))
  .map(x => x.inc(1)) // taken as an implicit
  .map(x => x.inc(2)) // again, no re-derivation
```

Note there is no need to import the companion object, the cached implicit TypeInformation is found when using the case class.
See [Semi-automatic derivation section](type-system.md#semi-automatic-derivation) for more details.
