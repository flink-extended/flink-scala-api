package org.apache.flinkx.api

import cats.data.NonEmptyList
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.serializers._

class CatsTest extends AnyFlatSpec with Matchers with TestUtils {
  implicit val stringListTi: TypeInformation[NonEmptyList[String]] = deriveTypeInformation
  implicit val intListTi: TypeInformation[NonEmptyList[Int]]       = deriveTypeInformation

  def createSerializer[T: TypeInformation] =
    implicitly[TypeInformation[T]].createSerializer(new ExecutionConfig())

  it should "derive for NEL[String]" in {
    val ser = createSerializer[NonEmptyList[String]]
    roundtrip(ser, NonEmptyList.one("doo"))
  }

  it should "derive for NEL[Int]" in {
    val ser = createSerializer[NonEmptyList[Int]]
    roundtrip(ser, NonEmptyList.one(1))
  }
}
