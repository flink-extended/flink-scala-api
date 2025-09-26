package org.apache.flinkx.api

import cats.data.NonEmptyList
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flinkx.api.SerializerTest.DeeplyNested.ModeNested.SuperNested.Egg
import org.apache.flinkx.api.SerializerTest.NestedRoot.NestedMiddle.NestedBottom
import org.apache.flinkx.api.SerializerTest._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, nullable}
import org.apache.flinkx.api.serializers._
import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.util.UUID

class SerializerTest extends AnyFlatSpec with Matchers with Inspectors with TestUtils {
  val ec = new SerializerConfigImpl()

  it should "derive serializer for simple class" in {
    testTypeInfoAndSerializer(Simple(1, "foo"))
  }

  it should "derive serializer for java classes" in {
    testSerializer(SimpleJava(1, "foo"))
  }

  it should "derive serializer for java.time classes" in {
    testTypeInfoAndSerializer(JavaTime(Instant.now(), LocalDate.now(), LocalDateTime.now()))
  }

  it should "derive nested classes" in {
    testTypeInfoAndSerializer(Nested(Simple(1, "foo")))
  }

  it should "derive for ADTs" in {
    testTypeInfoAndSerializer(Foo("a"), nullable = false)
    testTypeInfoAndSerializer(Bar(1), nullable = false)
  }

  it should "derive for ADTs with case objects" in {
    testTypeInfoAndSerializer(Foo2, nullable = false)
    testTypeInfoAndSerializer(Bar2, nullable = false)
  }

  it should "derive for deeply nested classes" in {
    testTypeInfoAndSerializer(Egg(1))
  }

  it should "derive for deeply nested adts" in {
    testTypeInfoAndSerializer(Egg(1), nullable = false)
  }

  it should "derive for nested ADTs" in {
    testTypeInfoAndSerializer(WrappedADT(Foo("a")))
    testTypeInfoAndSerializer(WrappedADT(Bar(1)))
  }

  it should "derive for generic ADTs" in {
    testTypeInfoAndSerializer(P2(1), nullable = false)
  }

  it should "derive seq" in {
    testTypeInfoAndSerializer(SimpleSeq(Seq(Simple(1, "a"))))
  }

  it should "derive list of ADT" in {
    testTypeInfoAndSerializer(List(Foo("a")), nullable = false)
    testSerializer(::(Foo("a"), Nil), nullable = false)
    testSerializer(Nil, nullable = false)
  }

  it should "derive list" in {
    testTypeInfoAndSerializer(List(Simple(1, "a")), nullable = false)
  }

  it should "derive nested list" in {
    testTypeInfoAndSerializer(List(SimpleList(List(1))), nullable = false)
  }

  it should "derive seq of seq" in {
    testTypeInfoAndSerializer(SimpleSeqSeq(Seq(Seq(Simple(1, "a")))))
  }

  it should "derive generic type bounded classes" in {
    testTypeInfoAndSerializer(BoundADT(Foo("a")))
  }

  //  it should "derive nested generic type bounded classes" in {
  //    val ser = implicitly[TypeInformation[NestedParent]].createSerializer(null)
  //    noKryo(ser)
  //  }

  it should "return the same TypeSerializer instance with case classes" in {
    val ti   = implicitly[TypeInformation[Simple]]
    val ser1 = ti.createSerializer(ec)
    val ser2 = ti.createSerializer(ec)
    ser1 should be theSameInstanceAs ser2
  }

  it should "be serializable in case of annotations on classes" in {
    val ser = implicitly[TypeInformation[Annotated]].createSerializer(ec)
    javaSerializable(ser)
  }

  it should "be serializable in case of annotations on subtypes" in {
    val ser = implicitly[TypeInformation[Ann]].createSerializer(ec)
    javaSerializable(ser)
  }

  it should "serialize Option" in {
    testTypeInfoAndSerializer(SimpleOption(None))
    testSerializer(SimpleOption(Some("foo")))
  }

  it should "serialize Either" in {
    testTypeInfoAndSerializer(SimpleEither(Left("foo")))
    testSerializer(SimpleEither(Right(42)))
  }

  it should "serialize nested list of ADT" in {
    testTypeInfoAndSerializer(ListADT(Nil))
    testSerializer(ListADT(List(Foo("a"))))
  }

  it should "derive multiple instances of generic class" in {
    testTypeInfoAndSerializer(Generic(SimpleOption(None), Bar(0)))
    testTypeInfoAndSerializer(Generic(Simple(0, "asd"), Bar(0)))
  }

  it should "serialize nil" in {
    testTypeInfoAndSerializer(NonEmptyList.one("a"))
  }

  it should "serialize unit" in {
    testTypeInfoAndSerializer((), nullable = false)
  }

  it should "serialize triple-nested case clases" in {
    testTypeInfoAndSerializer(List(NestedBottom(Some("a"), None)), nullable = false)
  }

  it should "serialize classes with type mapper" in {
    import MappedTypeInfoTest._
    val str = new WrappedString()
    str.put("foo")
    testTypeInfoAndSerializer(str, nullable = false)
  }

  it should "serialize bigint" in {
    testTypeInfoAndSerializer(BigInt(123), nullable = false)
  }

  it should "serialize bigdec" in {
    testTypeInfoAndSerializer(BigDecimal(123), nullable = false)
  }

  it should "serialize uuid" in {
    testTypeInfoAndSerializer(UUID.randomUUID(), nullable = false)
  }

  it should "serialize case class with private field" in {
    testTypeInfoAndSerializer(PrivateField("abc"))
  }

  it should "serialize a case class overriding a field" in {
    testTypeInfoAndSerializer(ExtendingCaseClass("abc", "def"))
  }

  it should "serialize a null case class" in {
    val data: Simple = null
    testTypeInfoAndSerializer(data)
  }

  it should "serialize a case class with nullable field" in {
    testTypeInfoAndSerializer(Bar(1))
  }

  it should "serialize a case class with a nullable field of a case class with no arity" in {
    testTypeInfoAndSerializer(NullableFieldWithNoArity(null))
  }

  it should "serialize nullable fields" in {
    val ser = implicitly[TypeInformation[SimpleJava]].createSerializer(ec)
    testTypeInfoAndSerializer(SimpleJava(null, null))
    val ccser = ser.asInstanceOf[CaseClassSerializer[SimpleJava]]
    // IntSerializer doesn't handle null so it's wrapped in a NullableSerializer
    ccser.getFieldSerializers()(0) shouldBe a[NullableSerializer[Integer]]
    ccser.getFieldSerializers()(1) shouldBe a[StringSerializer] // StringSerializer natively handles null
  }

  it should "serialize a case class with a nullable field of a fixed size case class" in {
    testTypeInfoAndSerializer(NullableFixedSizeCaseClass(null))
  }

  it should "serialize ZoneId" in {
    testTypeInfoAndSerializer(ZoneId.systemDefault())
  }

  it should "serialize ZoneOffset" in {
    // Don't test type info, as its arity and total fields depend on the JDK version
    testSerializer(ZoneOffset.UTC)
  }

  it should "serialize OffsetDateTime" in {
    // Don't test type info, as its arity and total fields depend on the JDK version
    testSerializer(OffsetDateTime.now())
  }

  it should "serialize ZonedDateTime" in {
    // Don't test type info, as its arity and total fields depend on the JDK version
    testSerializer(ZonedDateTime.now())
  }

}

object SerializerTest {
  case class Simple(a: Int, b: String)
  case class SimpleList(a: List[Int])
  case class SimpleJava(@nullable a: Integer, @nullable b: String)
  case class JavaTime(a: Instant, b: LocalDate, c: LocalDateTime)
  case class Nested(a: Simple)

  case class SimpleSeq(a: Seq[Simple])
  case class SimpleSeqSeq(a: Seq[Seq[Simple]])

  sealed trait ADT
  case class Foo(a: String) extends ADT
  case class Bar(b: Int)    extends ADT

  sealed trait ADT2
  case object Foo2 extends ADT2
  case object Bar2 extends ADT2

  case class WrappedADT(x: ADT)

  case class BoundADT[T <: ADT](x: T)

  sealed trait NestedParent
  case class NestedBoundADT[T <: ADT](x: T) extends NestedParent

  @SerialVersionUID(1L)
  case class Annotated(foo: String)

  sealed trait Ann
  @SerialVersionUID(1L)
  case class Next(foo: String) extends Ann

  object DeeplyNested {
    object ModeNested {
      object SuperNested {
        sealed trait Food
        case class Egg(i: Int) extends Food
        case object Ham        extends Food
      }
    }
  }

  sealed trait Param[T] {
    def a: T
  }
  case class P1(a: String) extends Param[String]
  case class P2(a: Int)    extends Param[Int]

  case class SimpleOption(a: Option[String])

  case class SimpleEither(a: Either[String, Int])

  case class Generic[T](a: T, b: ADT)

  case class ListADT(a: List[ADT])

  object NestedRoot {
    object NestedMiddle {
      case class NestedBottom(a: Option[String], b: Option[String])
    }
  }

  case class PrivateField(private val a: String)

  abstract class AbstractClass(val a: String)

  final case class ExtendingCaseClass(override val a: String, b: String) extends AbstractClass(a)

  final case class NullableField(var a: Foo = null, var b: Bar = null)

  object NullableField {
    def apply(a: Foo): NullableField = new NullableField(a, null)
  }

  final case class NoArity()

  final case class NullableFieldWithNoArity(var a: NoArity)

  final case class NullableFixedSizeCaseClass(@nullable javaTime: JavaTime)

}
