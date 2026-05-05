package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.FlinkRuntimeException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConstructorCompatTest extends AnyFlatSpec with Matchers {

  import ConstructorCompatTest.*
  import ConstructorCompatTest.EnumWithCaseClass.*
  import ConstructorCompatTest.EnumWithApplyInObject.*

  it should "lookup constructor with no parameter" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[NoParameter])
    constructor.apply(Array.empty) shouldBe a[NoParameter]
  }

  it should "lookup constructor with a boxed parameter" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[Boxed])
    constructor.apply(Array((1.asInstanceOf[AnyRef]))) shouldBe a[Boxed]
  }

  it should "lookup constructor with only default values" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[DefaultValues])
    constructor.apply(Array.empty) shouldBe a[DefaultValues]
  }

  it should "lookup constructor with an apply in the case class" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[ApplyInCaseClass])
    constructor.apply(Array.empty) shouldBe a[ApplyInCaseClass]
  }

  it should "lookup constructor with an apply in the companion object" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[ApplyInObject])
    constructor.apply(Array.empty) shouldBe a[ApplyInObject]
  }

  it should "lookup constructor with the longest apply in the companion object" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[LongestApplyInObject])
    constructor.apply(Array.empty) shouldBe a[LongestApplyInObject]
  }

  it should "lookup constructor with a secondary constructor" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[SecondaryConstructor])
    constructor.apply(Array.empty) shouldBe a[SecondaryConstructor]
  }

  it should "throw when the longest constructor is a secondary constructor" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[LongestSecondaryConstructor])
    val exception = intercept[IllegalArgumentException] {
      constructor.apply(Array.empty)
    }
    exception.getMessage shouldBe "wrong number of arguments: 2 expected: 3"
  }

  it should "lookup apply with an enum case class" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[EnumCaseClass])
    constructor.apply(Array("a", 2.asInstanceOf[AnyRef])) shouldBe a[EnumCaseClass]
  }

  it should "lookup apply with an enum case class with the longest apply in the companion object" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[EnumCaseClassWithLongestApplyInObject])
    constructor.apply(Array("a", 2.asInstanceOf[AnyRef])) shouldBe a[EnumCaseClassWithLongestApplyInObject]
  }

}

object ConstructorCompatTest {

  case object ConstructorCompatImpl extends ConstructorCompat

  case class NoParameter()

  case class Boxed(a: Int)

  case class DefaultValues(a: Int = 1, b: String = "b")

  case class ApplyInCaseClass(a: Int = 1, b: String = "b") {
    def apply(a: Int): ApplyInCaseClass = this.copy(a)
  }

  case class ApplyInObject(a: Int = 1, b: String = "b")

  object ApplyInObject {
    def apply(a: Int): ApplyInObject = new ApplyInObject(a)
  }

  case class LongestApplyInObject(a: Int = 1, b: String = "b")

  object LongestApplyInObject {
    def apply(a: Int, b: String, c: String): LongestApplyInObject = new LongestApplyInObject(a, b + c)
  }

  case class SecondaryConstructor(a: Int = 1, b: String = "b") {
    def this() = this(1)
  }

  case class LongestSecondaryConstructor(a: Int = 1, b: String = "b") {
    def this(a: Int, b: String, c: String) = this(a, b + c)
  }

  enum EnumWithCaseClass {
    case EnumCaseClass(a: String, b: Int)
    case Bar
  }

  enum EnumWithApplyInObject {
    case EnumCaseClassWithLongestApplyInObject(a: String, b: Int)
    case Bar
  }

  object EnumWithApplyInObject {
    object EnumCaseClassWithLongestApplyInObject {
      def apply(a: Int, b: String, c: String): EnumCaseClassWithLongestApplyInObject =
        new EnumCaseClassWithLongestApplyInObject(b + c, a)
    }
  }

}
