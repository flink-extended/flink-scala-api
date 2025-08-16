package org.apache.flinkx.api

import cats.data.NonEmptyList
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CatsTest extends AnyFlatSpec with Matchers with TestUtils {

  it should "derive for NEL[String]" in {
    NonEmptyList.one("doo") should haveTypeInfoAndBeSerializable[NonEmptyList[String]]
  }

  it should "derive for NEL[Int]" in {
    NonEmptyList.one(1) should haveTypeInfoAndBeSerializable[NonEmptyList[Int]]
  }
}
