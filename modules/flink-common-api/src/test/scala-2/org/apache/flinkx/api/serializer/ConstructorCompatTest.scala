package org.apache.flinkx.api.serializer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConstructorCompatTest extends AnyFlatSpec with Matchers {

  import ConstructorCompatTest._

  it should "lookup constructor with no parameter" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[NoParameter])
    constructor.apply(Array.empty) shouldBe NoParameter()
  }

  it should "lookup constructor with a boxed parameter" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[Boxed])
    constructor.apply(Array(Int.box(1))) shouldBe Boxed(1)
  }

  it should "append default values for missing parameters" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[DefaultValues])
    constructor.apply(Array(Int.box(2))) shouldBe DefaultValues(2, "b")
  }

  it should "lookup the primary constructor rather than a secondary constructor of another arity" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[SecondaryConstructor])
    constructor.apply(Array(Int.box(2), "c")) shouldBe SecondaryConstructor(2, "c")
  }

  it should "lookup the primary constructor rather than a secondary constructor of the same arity" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[SameAritySecondaryConstructor])
    constructor.apply(Array(Int.box(2), "c")) shouldBe SameAritySecondaryConstructor(2, "c")
  }

  it should "unbox value class parameters" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[WithValueClasses])
    constructor.apply(Array(box(UserId("u")), box(Age(3)), "s")) shouldBe WithValueClasses(UserId("u"), Age(3), "s")
  }

  it should "unbox a value class whose underlying value is private" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[WithPrivateValueClass])
    constructor.apply(Array(box(new Secret("s")))) shouldBe WithPrivateValueClass(new Secret("s"))
  }

  it should "not unbox a value class passed as a type parameter" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[Generic[UserId]])
    constructor.apply(Array(box(UserId("u")))) shouldBe Generic(UserId("u"))
  }

}

object ConstructorCompatTest {

  case object ConstructorCompatImpl extends ConstructorCompat

  // A value class is allocated when it is upcast, like a deserialized field
  private def box(value: Any): AnyRef = value.asInstanceOf[AnyRef]

  case class NoParameter()

  case class Boxed(a: Int)

  case class DefaultValues(a: Int = 1, b: String = "b")

  case class SecondaryConstructor(a: Int = 1, b: String = "b") {
    def this() = this(1)
  }

  case class SameAritySecondaryConstructor(a: Int, b: String) {
    def this(b: String, a: Int) = this(a + 1, b + "!")
  }

  case class UserId(value: String) extends AnyVal

  case class Age(value: Int) extends AnyVal

  class Secret(private val value: String) extends AnyVal {
    override def toString: String = s"Secret($value)"
  }

  case class WithValueClasses(id: UserId, age: Age, s: String)

  case class WithPrivateValueClass(secret: Secret)

  case class Generic[A](a: A)

}
