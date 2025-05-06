package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.serializer.CoproductSerializerTest.{Immutable, Immutable2, Immutable3, Mutable, Mutable1}
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoproductSerializerTest extends AnyFlatSpec with Matchers {

  it should "be immutable when all subtypes of the sealed trait are immutable" in {
    val immutableSerializer = implicitly[TypeSerializer[Immutable]]
    immutableSerializer.isImmutableType shouldEqual true
  }

  it should "be mutable when one subtype of the sealed trait is mutable" in {
    val mutableSerializer = implicitly[TypeSerializer[Mutable]]
    mutableSerializer.isImmutableType shouldEqual false
  }

  "copy" should "return null when the given object is null" in {
    val immutableSerializer = implicitly[TypeSerializer[Immutable]]

    val resultData = immutableSerializer.copy(null)

    resultData shouldEqual null
  }

  it should "return the same instance when immutable" in {
    val immutableSerializer = implicitly[TypeSerializer[Immutable]]
    val expectedData        = Immutable2("a")

    val resultData = immutableSerializer.copy(expectedData)

    resultData should be theSameInstanceAs expectedData
  }

  it should "copy the instance when mutable" in {
    val mutableSerializer = implicitly[TypeSerializer[Mutable]]
    val expectedData      = Mutable1("a")

    val resultData = mutableSerializer.copy(expectedData)

    resultData shouldNot be theSameInstanceAs expectedData
    resultData shouldEqual expectedData
  }

  it should "return the same instance when immutable even if some other subtypes of the sealed trait are mutable" in {
    val mutableSerializer = implicitly[TypeSerializer[Mutable]]
    val expectedData      = Immutable3("a")

    val resultData = mutableSerializer.copy(expectedData)

    resultData should be theSameInstanceAs expectedData
  }

}

object CoproductSerializerTest {

  sealed trait Immutable

  case class Immutable1(a: String) extends Immutable
  case class Immutable2(a: String) extends Immutable

  sealed trait Mutable

  case class Immutable3(a: String)   extends Mutable
  case class Mutable1(var a: String) extends Mutable

}
