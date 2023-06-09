package org.apache.flink.api

import cats.data.NonEmptyList
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.flink.api.serializers._

class CatsTest extends AnyFlatSpec with Matchers with TestUtils {

  it should "derive for NEL[String]" in {
    val ser = deriveTypeInformation[NonEmptyList[String]].createSerializer(null)
    roundtrip(ser, NonEmptyList.one("doo"))
  }
  it should "derive for NEL[Int]" in {
    val ser = deriveTypeInformation[NonEmptyList[Int]].createSerializer(null)
    roundtrip(ser, NonEmptyList.one(1))
  }
}
