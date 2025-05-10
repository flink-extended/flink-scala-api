package org.apache.flinkx.api.util

import org.apache.flinkx.api.util.ClassUtilTest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClassUtilTest extends AnyFlatSpec with Matchers {

  it should "return true when the field is a val" in {
    val aFinal = classOf[Final]
    ClassUtil.isFieldFinal(aFinal.getDeclaredFields, aFinal.getName, "a") shouldBe true
  }

  it should "return false when the field is a var" in {
    val aNonFinal = classOf[NonFinal]
    ClassUtil.isFieldFinal(aNonFinal.getDeclaredFields, aNonFinal.getName, "a") shouldBe false
  }

  it should "return true when the field is a private val" in {
    val aPrivateFinal = classOf[PrivateFinal]
    ClassUtil.isFieldFinal(aPrivateFinal.getDeclaredFields, aPrivateFinal.getName, "a") shouldBe true
  }

  it should "return false when the field is a private var" in {
    val aPrivateNonFinal = classOf[PrivateNonFinal]
    ClassUtil.isFieldFinal(aPrivateNonFinal.getDeclaredFields, aPrivateNonFinal.getName, "a") shouldBe false
  }

  it should "return true when the field is a disrupted private val" in {
    val aDisruptedPrivateFinal = classOf[DisruptedPrivateFinal]
    ClassUtil.isFieldFinal(aDisruptedPrivateFinal.getDeclaredFields, aDisruptedPrivateFinal.getName, "a") shouldBe true
  }

  it should "return false when the field is a disrupted private var" in {
    val aDisruptedPrivateNonFinal = classOf[DisruptedPrivateNonFinal]
    ClassUtil.isFieldFinal(aDisruptedPrivateNonFinal.getDeclaredFields, aDisruptedPrivateNonFinal.getName, "a") shouldBe false
  }

  it should "throw NoSuchFieldException when the field doesn't exist" in {
    val aFinal = classOf[Final]
    assertThrows[NoSuchFieldException] {
      ClassUtil.isFieldFinal(aFinal.getDeclaredFields, aFinal.getName, "wrongField") shouldBe true
    }
  }

}

object ClassUtilTest {

  case class Final(a: String)
  case class NonFinal(var a: String)
  case class PrivateFinal(private val a: String)
  case class PrivateNonFinal(private var a: String)
  object DisruptiveObject {
    def apply(value: Int): DisruptedPrivateFinal = DisruptedPrivateFinal(String.valueOf(value))
    def apply(value: Long): DisruptedPrivateNonFinal = DisruptedPrivateNonFinal(String.valueOf(value))
  }
  case class DisruptedPrivateFinal(private val a: String)
  case class DisruptedPrivateNonFinal(private var a: String)
}

