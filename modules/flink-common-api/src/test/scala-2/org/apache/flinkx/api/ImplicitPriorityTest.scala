package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.ImplicitPriorityTest.TestCase
import org.apache.flinkx.api.typeinfo.OptionTypeInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/** Test that verifies implicit priority ordering:
  *   - Implicits (specific types like tuples, collections) have higher priority
  *   - deriveTypeInformation has lower priority
  */
class ImplicitPriorityTest extends AnyFlatSpec with should.Matchers {

  "HighPrioImplicits" should "have higher priority than deriveTypeInformation" in {
    import org.apache.flinkx.api.auto._

    // This should use optionInfo from HighPrioImplicits, not deriveTypeInformation
    val ti: TypeInformation[Option[String]] = implicitly[TypeInformation[Option[String]]]

    ti shouldBe a[OptionTypeInfo[String, Option[String]]]
  }

  "semiauto.deriveTypeInformation" should "work when called explicitly" in {
    import org.apache.flinkx.api.semiauto._

    val ti: TypeInformation[TestCase] = deriveTypeInformation

    ti should not be null
  }

  "auto.deriveTypeInformation" should "be available as implicit" in {
    import org.apache.flinkx.api.auto._

    val ti: TypeInformation[TestCase] = implicitly[TypeInformation[TestCase]]

    ti should not be null
  }

}

object ImplicitPriorityTest {

  case class TestCase(name: Option[String])

}
