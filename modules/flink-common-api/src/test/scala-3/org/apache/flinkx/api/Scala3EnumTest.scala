package org.apache.flinkx.api

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.auto.*

class Scala3EnumTest extends AnyFlatSpec with Matchers with TestUtils {

  import Scala3EnumTest.Failure
  import Scala3EnumTest.Failure.*
  import Scala3EnumTest.FailureCategory
  import Scala3EnumTest.FailureCategory.*
  import Scala3EnumTest.Example
  import Scala3EnumTest.Example.*
  import Scala3EnumTest.FailureEvent
  import Scala3EnumTest.FooEvent

  it should "derive type information for a Scala 3 enum" in {
    summon[TypeInformation[Example]] shouldNot be(null)
  }

  it should "derive type information for a Scala 3 enum value" in {
    summon[TypeInformation[Foo]] shouldNot be(null)
  }

  it should "roundtrip a enum" in {
    testTypeInfoAndSerializer(PARSE_ERROR, false)
  }

  it should "roundtrip an enum with parameter" in {
    testTypeInfoAndSerializer(PARSING, false)
  }

  it should "roundtrip an enum with a simple case" in {
    testTypeInfoAndSerializer(Bar, false)
  }

  it should "roundtrip an enum with a case with parameters" in {
    testTypeInfoAndSerializer[Example](Foo("a", 2))
  }

  it should "roundtrip an enum value with parameters" in {
    testTypeInfoAndSerializer[Foo](Foo("a", 2))
  }

  it should "roundtrip a case class declaring an enum" in {
    testTypeInfoAndSerializer(FailureEvent("a", PARSING))
  }

  it should "roundtrip a case class declaring an enum value" in {
    testTypeInfoAndSerializer(FooEvent("a", Foo("a", 2)))
  }

}

object Scala3EnumTest {

  enum Failure {
    case MISSING_KEY, PARSE_ERROR, UNKNOWN
  }

  enum FailureCategory(a: Int) {
    case MISSING extends FailureCategory(1)
    case PARSING extends FailureCategory(2)
    case OTHER extends FailureCategory(3)
  }

  enum Example {
    case Foo(a: String, b: Int)
    case Bar
  }

  case class FailureEvent(step: String, category: FailureCategory)

  case class FooEvent(step: String, foo: Example.Foo)

}
