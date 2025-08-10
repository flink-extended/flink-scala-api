package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
import org.apache.flinkx.api.serializer.CoproductSerializerTest._
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoproductSerializerTest extends AnyFlatSpec with Matchers {

  "isImmutableType" should "be true when all subtypes of the sealed trait are immutable" in {
    val immutableSerializer = implicitly[TypeSerializer[Immutable]]
    immutableSerializer.isImmutableType shouldEqual true
  }

  it should "be false when one subtype of the sealed trait is mutable" in {
    val mutableSerializer = implicitly[TypeSerializer[Mutable]]
    mutableSerializer.isImmutableType shouldEqual false
  }

  "isImmutableSerializer" should "be true when all sub-serializers of the sealed trait are immutable" in {
    val immutableSerializer = implicitly[TypeSerializer[Immutable]]
    immutableSerializer.asInstanceOf[CoproductSerializer[Immutable]].isImmutableSerializer shouldEqual true
  }

  it should "be false when one sub-serializer of the sealed trait is mutable" in {
    implicit val rowInfo: RowTypeInfo = new RowTypeInfo()
    val mutableSerializer             = implicitly[TypeSerializer[MutableSerializer]]
    mutableSerializer.asInstanceOf[CoproductSerializer[MutableSerializer]].isImmutableSerializer shouldEqual false
  }

  "copy" should "return null when the given object is null" in {
    val immutableSerializer = implicitly[TypeSerializer[Immutable]]

    val resultData = immutableSerializer.copy(null)

    resultData shouldEqual null
  }

  it should "return the same instance when the data is immutable" in {
    val immutableSerializer = implicitly[TypeSerializer[Immutable]]
    val expectedData        = Immutable2("a")

    val resultData = immutableSerializer.copy(expectedData)

    resultData should be theSameInstanceAs expectedData
  }

  it should "copy the instance when the data is mutable" in {
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

  "duplicate" should "return itself when the serializer is immutable" in {
    val immutableSerializer = implicitly[TypeSerializer[Mutable]]

    val duplicatedSerializer = immutableSerializer.duplicate()

    duplicatedSerializer should be theSameInstanceAs immutableSerializer
  }

  it should "return a new instance of itself when the serializer is mutable" in {
    implicit val rowInfo: RowTypeInfo = new RowTypeInfo()
    val serializer                    = implicitly[TypeSerializer[MutableSerializer]]

    val duplicatedSerializer = serializer.duplicate()

    duplicatedSerializer shouldNot be theSameInstanceAs serializer
    duplicatedSerializer should be(serializer)
  }

}

object CoproductSerializerTest {

  sealed trait Immutable

  case class Immutable1(a: String) extends Immutable
  case class Immutable2(a: String) extends Immutable

  sealed trait Mutable

  case class Immutable3(a: String)   extends Mutable
  case class Mutable1(var a: String) extends Mutable

  sealed trait MutableSerializer

  case class Immutable4(a: String)      extends MutableSerializer
  case class MutableSerializer1(a: Row) extends MutableSerializer

}
