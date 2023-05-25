package org.apache.flinkadt

import org.apache.flinkadt.TypeInfoTest.{ADT, ListedArray, ListedList, ListedMap, Parameterized, Simple}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flinkadt.api.implicits._

class TypeInfoTest extends AnyFlatSpec with Matchers {

  it should "derive simple classes" in {
    drop(deriveTypeInformation[Simple])
  }

  it should "derive parameterized classes" in {
    drop(deriveTypeInformation[Parameterized[String]])
  }

  it should "derive lists" in {
    drop(deriveTypeInformation[ListedList])
  }

  it should "derive arrays" in {
    drop(deriveTypeInformation[ListedArray])
  }

  it should "derive maps" in {
    drop(deriveTypeInformation[ListedMap])
  }

  it should "derive ADT" in {
    drop(deriveTypeInformation[ADT])
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
