package org.apache.flinkx.api.util

import org.apache.flinkx.api.util.ClassUtilTest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClassUtilTest extends AnyFlatSpec with Matchers {

  it should "return true when all the fields are val" in {
    val anImmutable = classOf[Immutable]
    ClassUtil.isCaseClassImmutable(anImmutable, Seq("a", "b")) shouldBe true
  }

  it should "return false when one of the fields is a var" in {
    val aMutable = classOf[Mutable]
    ClassUtil.isCaseClassImmutable(aMutable, Seq("a", "b")) shouldBe false
  }

  it should "return true when all the fields are private val" in {
    val aPrivateImmutable = classOf[PrivateImmutable]
    ClassUtil.isCaseClassImmutable(aPrivateImmutable, Seq("a", "b")) shouldBe true
  }

  it should "return false when one of the fields is a private var" in {
    val aPrivateMutable = classOf[PrivateMutable]
    ClassUtil.isCaseClassImmutable(aPrivateMutable, Seq("a", "b")) shouldBe false
  }

  it should "return true when the field is a disrupted private val" in {
    val aDisruptedPrivateImmutable = classOf[DisruptedPrivateImmutable]
    ClassUtil.isCaseClassImmutable(aDisruptedPrivateImmutable, Seq("a", "b")) shouldBe true
  }

  it should "return false when the field is a disrupted private var" in {
    val aDisruptedPrivateMutable = classOf[DisruptedPrivateMutable]
    ClassUtil.isCaseClassImmutable(aDisruptedPrivateMutable, Seq("a", "b")) shouldBe false
  }

  it should "return true when the fields are in parent classes" in {
    val anExtendingImmutable = classOf[ExtendingImmutable]
    ClassUtil.isCaseClassImmutable(anExtendingImmutable, Seq("a", "b", "c")) shouldBe true
  }

  it should "return false when one of the fields which is not in parent classes is a var" in {
    val anExtendingMutable = classOf[ExtendingMutable]
    ClassUtil.isCaseClassImmutable(anExtendingMutable, Seq("a", "b", "c")) shouldBe false
  }

  it should "return true when the field doesn't exist" in {
    val anImmutable = classOf[Immutable]
    ClassUtil.isCaseClassImmutable(anImmutable, Seq("wrongField")) shouldBe true
  }

}

object ClassUtilTest {

  case class Immutable(a: String, b: String)
  case class Mutable(a: String, var b: String)
  case class PrivateImmutable(private val a: String, private val b: String)
  case class PrivateMutable(private val a: String, private var b: String)
  object DisruptiveObject {
    def apply(value: Int): DisruptedPrivateImmutable = DisruptedPrivateImmutable(String.valueOf(value))
    def apply(value: Long): DisruptedPrivateMutable  = DisruptedPrivateMutable(String.valueOf(value))
  }
  case class DisruptedPrivateImmutable(private val a: String)
  case class DisruptedPrivateMutable(private var a: String)
  abstract class AbstractClass(val a: String) // var variant is not possible: "Mutable variable cannot be overridden"
  class IntermediateClass(override val a: String, val b: String) extends AbstractClass(a)
  case class ExtendingImmutable(override val a: String, override val b: String, c: String)
      extends IntermediateClass(a, b)
  case class ExtendingMutable(override val a: String, override val b: String, var c: String)
      extends IntermediateClass(a, b)
}
