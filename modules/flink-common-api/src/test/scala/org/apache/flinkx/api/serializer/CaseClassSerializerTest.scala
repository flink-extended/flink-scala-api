package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flinkx.api.serializer.CaseClassSerializerTest.{Immutable, Mutable, OuterImmutable, OuterMutable}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CaseClassSerializerTest extends AnyFlatSpec with Matchers {

  it should "be immutable when parameters are immutable" in {
    val serializer = new CaseClassSerializer[Immutable](classOf[Immutable], Array(StringSerializer.INSTANCE), true)
    serializer.isImmutableType should be(true)
  }

  it should "be mutable when one parameter is mutable" in {
    val serializer = new CaseClassSerializer[Mutable](classOf[Mutable], Array(StringSerializer.INSTANCE), false)
    serializer.isImmutableType should be(false)
  }

  it should "be mutable when the content of one parameter is mutable" in {
    val mutableSerializer = new CaseClassSerializer[Mutable](classOf[Mutable], Array(StringSerializer.INSTANCE), false)
    val serializer = new CaseClassSerializer[OuterImmutable](classOf[OuterImmutable], Array(mutableSerializer), true)
    mutableSerializer.isImmutableType should be(false)
  }

  it should "be mutable when missing information about one parameter" in {
    val serializer = new CaseClassSerializer[Immutable](classOf[Immutable], Array(null), true)
    serializer.isImmutableType should be(false)
  }

  "copy" should "return the same case class when immutable" in {
    val immutableSerializer =
      new CaseClassSerializer[Immutable](classOf[Immutable], Array(StringSerializer.INSTANCE), true)
    val expectedData = Immutable("a")

    val resultData = immutableSerializer.copy(expectedData)

    resultData should be theSameInstanceAs expectedData
  }

  it should "return null when the given case class is null" in {
    val immutableSerializer =
      new CaseClassSerializer[Immutable](classOf[Immutable], Array(StringSerializer.INSTANCE), true)

    val resultData = immutableSerializer.copy(null)

    resultData shouldEqual null
  }

  it should "copy the case class when mutable" in {
    val mutableSerializer = new CaseClassSerializer[Mutable](classOf[Mutable], Array(StringSerializer.INSTANCE), false)
    val expectedData      = Mutable("a")

    val resultData = mutableSerializer.copy(expectedData)

    resultData shouldNot be theSameInstanceAs expectedData
    resultData shouldEqual expectedData
  }

  it should "copy the case class and its content when the content of one parameter is mutable" in {
    val mutableSerializer = new CaseClassSerializer[Mutable](classOf[Mutable], Array(StringSerializer.INSTANCE), false)
    val outerImmutableSerializer =
      new CaseClassSerializer[OuterImmutable](classOf[OuterImmutable], Array(mutableSerializer), true)
    val expectedData = OuterImmutable(Mutable("a"))

    val resultData = outerImmutableSerializer.copy(expectedData)

    resultData shouldNot be theSameInstanceAs expectedData
    resultData.a shouldNot be theSameInstanceAs expectedData.a
    resultData shouldEqual expectedData
  }

  it should "copy the case class when mutable but not its immutable content" in {
    val immutableSerializer =
      new CaseClassSerializer[Immutable](classOf[Immutable], Array(StringSerializer.INSTANCE), true)
    val outerMutableSerializer =
      new CaseClassSerializer[OuterMutable](classOf[OuterMutable], Array(immutableSerializer), false)
    val expectedData = OuterMutable(Immutable("a"))

    val resultData = outerMutableSerializer.copy(expectedData)

    resultData shouldNot be theSameInstanceAs expectedData
    resultData.a should be theSameInstanceAs expectedData.a
    resultData shouldEqual expectedData
  }

}

object CaseClassSerializerTest {

  case class Immutable(a: String) {
    var b: String = "" // This field is not taken into account because it's not serialized.
  }
  case class Mutable(var a: String) {
    val b: String = ""
  }

  case class OuterImmutable(a: Mutable)

  case class OuterMutable(var a: Immutable)

}
