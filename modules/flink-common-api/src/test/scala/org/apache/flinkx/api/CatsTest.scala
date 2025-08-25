package org.apache.flinkx.api

import cats.data.NonEmptyList
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CatsTest extends AnyFlatSpec with Matchers with TestUtils {

  it should "derive for NEL[String]" in {
    testSerializer(NonEmptyList.one("doo"))
  }

  it should "derive for NEL[Int]" in {
    testSerializer(NonEmptyList.one(1))
  }

}
