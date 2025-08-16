package org.apache.flinkx.api

import cats.data.NonEmptyList
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flinkx.api.SerializerTest.DeeplyNested.ModeNested.SuperNested.{Egg, Food, Ham}
import org.apache.flinkx.api.SerializerTest.NestedRoot.NestedMiddle.NestedBottom
import org.apache.flinkx.api.SerializerTest._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, nullable}
import org.apache.flinkx.api.serializers._
import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.UUID

class SerializerTest extends AnyFlatSpec with Matchers with Inspectors with TestUtils {
  val ec = new SerializerConfigImpl()

  it should "derive serializer for simple class" in {
    Simple(1, "foo") should haveTypeInfoAndBeSerializable[Simple]
  }

  it should "derive serializer for java classes" in {
    SimpleJava(1, "foo") should beSerializable[SimpleJava]
  }

  it should "derive serializer for java.time classes" in {
    JavaTime(Instant.now(), LocalDate.now(), LocalDateTime.now()) should haveTypeInfoAndBeSerializable[JavaTime]
  }

  it should "derive nested classes" in {
    Nested(Simple(1, "foo")) should haveTypeInfoAndBeSerializable[Nested]
  }

  it should "derive for ADTs" in {
    Foo("a") should haveTypeInfoAndBeSerializable[ADT](nullable = false)
    Bar(1) should haveTypeInfoAndBeSerializable[ADT](nullable = false)
  }

  it should "derive for ADTs with case objects" in {
    Foo2 should haveTypeInfoAndBeSerializable[ADT2](nullable = false)
    Bar2 should haveTypeInfoAndBeSerializable[ADT2](nullable = false)
  }

  it should "derive for deeply nested classes" in {
    Egg(1) should haveTypeInfoAndBeSerializable[Egg]
  }

  it should "derive for deeply nested adts" in {
    Egg(1) should haveTypeInfoAndBeSerializable[Food](nullable = false)
  }

  it should "derive for nested ADTs" in {
    WrappedADT(Foo("a")) should haveTypeInfoAndBeSerializable[WrappedADT]
    WrappedADT(Bar(1)) should haveTypeInfoAndBeSerializable[WrappedADT]
  }

  it should "derive for generic ADTs" in {
    P2(1) should haveTypeInfoAndBeSerializable[Param[Int]](nullable = false)
  }

  it should "derive seq" in {
    SimpleSeq(Seq(Simple(1, "a"))) should haveTypeInfoAndBeSerializable[SimpleSeq]
  }

  it should "derive list of ADT" in {
    List(Foo("a")) should haveTypeInfoAndBeSerializable[List[ADT]](nullable = false)
    ::(Foo("a"), Nil) should beSerializable[List[ADT]](nullable = false)
    Nil should beSerializable[List[ADT]](nullable = false)
  }

  it should "derive recursively" in {
    // recursive is broken
    // val ti = implicitly[TypeInformation[Node]]
  }

  it should "derive list" in {
    List(Simple(1, "a")) should haveTypeInfoAndBeSerializable[List[Simple]](nullable = false)
  }

  it should "derive nested list" in {
    List(SimpleList(List(1))) should haveTypeInfoAndBeSerializable[List[SimpleList]](nullable = false)
  }

  it should "derive seq of seq" in {
    SimpleSeqSeq(Seq(Seq(Simple(1, "a")))) should haveTypeInfoAndBeSerializable[SimpleSeqSeq]
  }

  it should "derive generic type bounded classes" in {
    BoundADT(Foo("a")) should haveTypeInfoAndBeSerializable[BoundADT[Foo]]
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
    SimpleOption(None) should haveTypeInfoAndBeSerializable[SimpleOption]
    SimpleOption(Some("foo")) should beSerializable[SimpleOption]
  }

  it should "serialize Either" in {
    SimpleEither(Left("foo")) should haveTypeInfoAndBeSerializable[SimpleEither]
    SimpleEither(Right(42)) should beSerializable[SimpleEither]
  }

  it should "serialize nested list of ADT" in {
    ListADT(Nil) should haveTypeInfoAndBeSerializable[ListADT]
    ListADT(List(Foo("a"))) should beSerializable[ListADT]
  }

  it should "derive multiple instances of generic class" in {
    Generic(SimpleOption(None), Bar(0)) should haveTypeInfoAndBeSerializable[Generic[SimpleOption]]
    Generic(Simple(0, "asd"), Bar(0)) should haveTypeInfoAndBeSerializable[Generic[Simple]]
  }

  it should "serialize nil" in {
    NonEmptyList.one("a") should haveTypeInfoAndBeSerializable[NonEmptyList[String]]
  }

  it should "serialize unit" in {
    () should haveTypeInfoAndBeSerializable[Unit](nullable = false)
  }

  it should "serialize triple-nested case clases" in {
    List(NestedBottom(Some("a"), None)) should haveTypeInfoAndBeSerializable[Seq[NestedBottom]](nullable = false)
  }

  it should "serialize classes with type mapper" in {
    import MappedTypeInfoTest._
    val str = new WrappedString()
    str.put("foo")
    str should haveTypeInfoAndBeSerializable[WrappedString](nullable = false)
  }

  it should "serialize bigint" in {
    BigInt(123) should haveTypeInfoAndBeSerializable[BigInt](nullable = false)
  }

  it should "serialize bigdec" in {
    BigDecimal(123) should haveTypeInfoAndBeSerializable[BigDecimal](nullable = false)
  }

  it should "serialize uuid" in {
    UUID.randomUUID() should haveTypeInfoAndBeSerializable[UUID](nullable = false)
  }

  it should "serialize case class with private field" in {
    PrivateField("abc") should haveTypeInfoAndBeSerializable[PrivateField]
  }

  it should "serialize a case class overriding a field" in {
    ExtendingCaseClass("abc", "def") should haveTypeInfoAndBeSerializable[ExtendingCaseClass]
  }

  it should "serialize a null case class" in {
    val data: Simple = null
    data should haveTypeInfoAndBeSerializable[Simple]
  }

  it should "serialize a case class with nullable field" in {
    Bar(1) should haveTypeInfoAndBeSerializable[Bar]
  }

  it should "serialize a case class with a nullable field of a case class with no arity" in {
    NullableFieldWithNoArity(null) should haveTypeInfoAndBeSerializable[NullableFieldWithNoArity]
  }

  it should "serialize nullable fields" in {
    val ser = implicitly[TypeInformation[SimpleJava]].createSerializer(ec)
    SimpleJava(null, null) should haveTypeInfoAndBeSerializable[SimpleJava]
    val ccser = ser.asInstanceOf[CaseClassSerializer[SimpleJava]]
    // IntSerializer doesn't handle null so it's wrapped in a NullableSerializer
    ccser.getFieldSerializers()(0) shouldBe a[NullableSerializer[Integer]]
    ccser.getFieldSerializers()(1) shouldBe a[StringSerializer] // StringSerializer natively handles null
  }

  it should "serialize a case class with a nullable field of a fixed size case class" in {
    NullableFixedSizeCaseClass(null) should haveTypeInfoAndBeSerializable[NullableFixedSizeCaseClass]
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

  case class Node(left: Option[Node], right: Option[Node])

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
