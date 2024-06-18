package org.apache.flinkx.api

import cats.data.NonEmptyList
import org.apache.flinkx.api.serializers._
import org.apache.flinkx.api.AnyTest._
import org.apache.flinkx.api.AnyTest.FAny.FValueAny.FTerm
import org.apache.flinkx.api.AnyTest.FAny.FValueAny.FTerm.StringTerm
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.ExecutionConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AnyTest extends AnyFlatSpec with Matchers with TestUtils {
  val ec = new ExecutionConfig()

  def createSerializer[T: TypeInformation] =
    implicitly[TypeInformation[T]].createSerializer(ec)

  it should "serialize concrete class" in {
    val ser = createSerializer[StringTerm]
    roundtrip(ser, StringTerm("fo"))
  }

  it should "serialize ADT" in {
    val ser = createSerializer[FAny]
    roundtrip(ser, StringTerm("fo"))
  }

  it should "serialize NEL" in {
    val ser = createSerializer[NonEmptyList[FTerm]]
    roundtrip(ser, NonEmptyList.one(StringTerm("fo")))
  }

  it should "serialize nested nel" in {
    val ser = createSerializer[TermFilter]
    roundtrip(ser, TermFilter("a", NonEmptyList.one(StringTerm("fo"))))
  }
}

object AnyTest {
  sealed trait FAny

  object FAny {
    sealed trait FValueAny extends FAny {
      def value: Any
    }

    object FValueAny {
      sealed trait FTerm extends FValueAny

      object FTerm {
        case class StringTerm(value: String) extends FTerm {
          type T = String
        }

        object StringTerm {
          implicit val stringTermTi: TypeInformation[StringTerm] = deriveTypeInformation
        }

        case class NumericTerm(value: Double) extends FTerm {
          type T = Double
        }

        object NumericTerm {
          implicit val numericTermTi: TypeInformation[NumericTerm] = deriveTypeInformation
        }

        implicit val fTermTi: TypeInformation[FTerm] = deriveTypeInformation
      }

      implicit val fValueAnyTi: TypeInformation[FValueAny] = deriveTypeInformation
    }

    implicit val fAnyTi: TypeInformation[FAny] = deriveTypeInformation
  }

  case class TermFilter(field: String, values: NonEmptyList[FTerm])

  object TermFilter {
    implicit val termFilterTi: TypeInformation[TermFilter] = deriveTypeInformation
  }
}
