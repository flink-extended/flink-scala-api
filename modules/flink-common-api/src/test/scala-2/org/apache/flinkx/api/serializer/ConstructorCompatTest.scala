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

  it should "lookup a private primary constructor rather than a public secondary constructor of the same arity" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[PrivatePrimaryConstructor])
    constructor.apply(Array("a", "b")) shouldBe PrivatePrimaryConstructor("a", "b")
  }

  it should "create the instance with Java reflection" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[JavaInstantiated])
    constructor.apply(Array(Int.box(1))).viaScalaReflection shouldBe false
  }

  it should "create the instance with Scala reflection when a parameter is a value class" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[ScalaInstantiated])
    constructor.apply(Array(box(UserId("u")))).viaScalaReflection shouldBe true
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

  it should "unbox a value class referred to by a type alias" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[WithAliasedValueClass])
    val instance    = constructor.apply(Array(box(UserId("u"))))
    instance shouldBe WithAliasedValueClass(UserId("u"))
    instance.viaScalaReflection shouldBe true
  }

  it should "unbox a value class with a type parameter" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[WithGenericValueClass])
    val instance    = constructor.apply(Array(box(Tagged("t"))))
    instance shouldBe WithGenericValueClass(Tagged("t"))
    instance.viaScalaReflection shouldBe true
  }

  it should "not unbox a value class nested in another type" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[WithNestedValueClasses])
    val instance    = constructor.apply(Array(Some(UserId("u")), List(UserId("v"))))
    instance shouldBe WithNestedValueClasses(Some(UserId("u")), List(UserId("v")))
    instance.viaScalaReflection shouldBe false
  }

  it should "lookup the constructor of a value class" in {
    val constructor = ConstructorCompatImpl.lookupConstructor(classOf[UserId])
    constructor.apply(Array("u")) shouldBe UserId("u")
  }

  it should "create the instance with Java reflection when the constructor is protected or qualified private" in {
    val protectedConstructor = ConstructorCompatImpl.lookupConstructor(classOf[ProtectedConstructor])
    protectedConstructor.apply(Array(Int.box(1))).viaScalaReflection shouldBe false
    val qualifiedPrivateConstructor = ConstructorCompatImpl.lookupConstructor(classOf[QualifiedPrivateConstructor])
    qualifiedPrivateConstructor.apply(Array(Int.box(1))).viaScalaReflection shouldBe false
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

  // The custom apply doesn't call the primary constructor, which stays private in the bytecode
  case class PrivatePrimaryConstructor private (a: String, b: AnyRef) {
    def this(b: AnyRef, a: String) = this(a, b)
  }

  object PrivatePrimaryConstructor {
    def apply(a: String, b: AnyRef): PrivatePrimaryConstructor = new PrivatePrimaryConstructor(b, a)
  }

  case class UserId(value: String) extends AnyVal

  case class Age(value: Int) extends AnyVal

  class Secret(private val value: String) extends AnyVal {
    override def toString: String = s"Secret($value)"
  }

  case class WithValueClasses(id: UserId, age: Age, s: String)

  case class WithPrivateValueClass(secret: Secret)

  case class Generic[A](a: A)

  case class Tagged[A](value: A) extends AnyVal

  type AliasedUserId = UserId

  trait StackRecorder {
    val viaScalaReflection: Boolean = new Throwable().getStackTrace.exists(_.getClassName.startsWith("scala.reflect"))
  }

  case class JavaInstantiated(a: Int) extends StackRecorder

  case class ScalaInstantiated(id: UserId) extends StackRecorder

  case class WithAliasedValueClass(id: AliasedUserId) extends StackRecorder

  case class WithGenericValueClass(tagged: Tagged[String]) extends StackRecorder

  case class WithNestedValueClasses(id: Option[UserId], ids: List[UserId]) extends StackRecorder

  // Scala emits both constructors as public in the bytecode
  case class ProtectedConstructor protected (a: Int) extends StackRecorder

  case class QualifiedPrivateConstructor private[serializer] (a: Int) extends StackRecorder

}
