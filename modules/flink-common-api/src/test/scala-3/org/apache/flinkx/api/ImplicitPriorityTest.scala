package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.flinkx.api.typeinfo.OptionTypeInfo

import scala.compiletime.testing.typeCheckErrors

/** Test that verifies implicit priority ordering:
  *  - Implicits (specific types like primitives, collections) have higher priority
  *  - deriveTypeInformation has lower priority
  */
class ImplicitPriorityTest extends AnyFlatSpec with should.Matchers {

  "HighPrioImplicits" should "have higher priority than deriveTypeInformation" in {
    import org.apache.flinkx.api.auto.{*, given}

    // This should use optionInfo from HighPrioImplicits, not deriveTypeInformation
    val ti: TypeInformation[Option[String]] = summon[TypeInformation[Option[String]]]

    ti shouldBe a[OptionTypeInfo[String, Option[String]]]
  }

  "semiauto.deriveTypeInformation" should "not be available as implicit" in {
    // This should fail to compile because deriveTypeInformation in semiauto is NOT implicit
    val errors = typeCheckErrors("""
      import org.apache.flinkx.api.semiauto.{*, given}

      case class TestCase(name: String, value: Int)

      // This should fail because deriveTypeInformation is not implicit in semiauto
      val ti: TypeInformation[TestCase] = summon[TypeInformation[TestCase]]
    """)

    // Should have a compilation error about missing implicit
    errors should not be empty
    errors.head.message should include("no implicit")
  }

  it should "work when called explicitly" in {
    import org.apache.flinkx.api.semiauto.{*, given}
    import scala.reflect.ClassTag

    case class TestCase(name: String, value: Int)

    // This should work - calling deriveTypeInformation explicitly
    val ti: TypeInformation[TestCase] = deriveTypeInformation[TestCase]

    ti should not be null
  }

  "auto.deriveTypeInformation" should "be available as implicit" in {
    import org.apache.flinkx.api.auto.{*, given}

    case class TestCase(name: String, value: Int)

    // This should work because deriveTypeInformation is implicit in auto
    val ti: TypeInformation[TestCase] = summon[TypeInformation[TestCase]]

    ti should not be null
  }

}
