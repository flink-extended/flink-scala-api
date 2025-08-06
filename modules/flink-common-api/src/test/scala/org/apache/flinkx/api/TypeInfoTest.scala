package org.apache.flinkx.api

import org.apache.flinkx.api.TypeInfoTest._
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypeInfoTest extends AnyFlatSpec with Matchers {

  it should "derive simple classes" in {
    deriveTypeInformation[Simple] shouldNot be(null)
  }

  it should "derive parameterized classes" in {
    deriveTypeInformation[Parameterized[String]] shouldNot be(null)
  }

  it should "derive lists" in {
    deriveTypeInformation[ListedList] shouldNot be(null)
  }

  it should "derive arrays" in {
    deriveTypeInformation[ListedArray] shouldNot be(null)
  }

  it should "derive maps" in {
    deriveTypeInformation[ListedMap] shouldNot be(null)
  }

  it should "derive ADT" in {
    deriveTypeInformation[ADT] shouldNot be(null)
  }
}

object TypeInfoTest {
  case class Parameterized[T](a: T)
  case class Simple(a: String, b: Int)
  case class ListedList(x: List[String])
  case class ListedArray(x: Array[String])
  case class ListedMap(x: Map[String, String])

  sealed trait ADT
  case class Foo(a: String) extends ADT
  case class Bar(a: Int)    extends ADT
}
