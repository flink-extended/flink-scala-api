package org.apache.flinkx.api

import cats.data.NonEmptyList
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.SerializerTest.DeeplyNested.ModeNested.SuperNested.{Egg, Food}
import org.apache.flinkx.api.SerializerTest.NestedRoot.NestedMiddle.NestedBottom
import org.apache.flinkx.api.SerializerTest._
import org.apache.flinkx.api.serializers._
import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.UUID

class SerializerTest extends AnyFlatSpec with Matchers with Inspectors with TestUtils {
  val ec = new ExecutionConfig()

  it should "derive serializer for simple class" in {
    val ser = implicitly[TypeInformation[Simple]].createSerializer(ec)
    all(ser, Simple(1, "foo"))
  }

  it should "derive serializer for java classes" in {
    val ser = implicitly[TypeInformation[SimpleJava]].createSerializer(ec)
    all(ser, SimpleJava(1, "foo"))
  }

  it should "derive serializer for java.time classes" in {
    val ser = implicitly[TypeInformation[JavaTime]].createSerializer(ec)
    all(ser, JavaTime(Instant.now(), LocalDate.now(), LocalDateTime.now()))
  }

  it should "derive nested classes" in {
    val ser = implicitly[TypeInformation[Nested]].createSerializer(ec)
    all(ser, Nested(Simple(1, "foo")))
  }

  it should "derive for ADTs" in {
    val ser = implicitly[TypeInformation[ADT]].createSerializer(ec)
    all(ser, Foo("a"))
    all(ser, Bar(1))
  }

  it should "derive for ADTs with case objects" in {
    val ser = implicitly[TypeInformation[ADT2]].createSerializer(ec)
    all(ser, Foo2)
    all(ser, Bar2)
  }

  it should "derive for deeply nested classes" in {
    val ser = implicitly[TypeInformation[Egg]].createSerializer(ec)
    all(ser, Egg(1))
  }

  it should "derive for deeply nested adts" in {
    val ser = implicitly[TypeInformation[Food]].createSerializer(ec)
    all(ser, Egg(1))
  }

  it should "derive for nested ADTs" in {
    val ser = implicitly[TypeInformation[WrappedADT]].createSerializer(ec)
    all(ser, WrappedADT(Foo("a")))
    all(ser, WrappedADT(Bar(1)))
  }

  it should "derive for generic ADTs" in {
    val ser = implicitly[TypeInformation[Param[Int]]].createSerializer(ec)
    all(ser, P2(1))
  }

  it should "derive seq" in {
    val ser = implicitly[TypeInformation[SimpleSeq]].createSerializer(ec)
    noKryo(ser)
    serializable(ser)
  }

  it should "derive list of ADT" in {
    val ser = implicitly[TypeInformation[List[ADT]]].createSerializer(ec)
    all(ser, List(Foo("a")))
    roundtrip(ser, ::(Foo("a"), Nil))
    roundtrip(ser, Nil)
  }

  it should "derive recursively" in {
    // recursive is broken
    // val ti = implicitly[TypeInformation[Node]]
  }

  it should "derive list" in {
    val ser = implicitly[TypeInformation[List[Simple]]].createSerializer(ec)
    all(ser, List(Simple(1, "a")))
  }

  it should "derive nested list" in {
    val ser = implicitly[TypeInformation[List[SimpleList]]].createSerializer(ec)
    all(ser, List(SimpleList(List(1))))
  }

  it should "derive seq of seq" in {
    val ser = implicitly[TypeInformation[SimpleSeqSeq]].createSerializer(ec)
    noKryo(ser)
    serializable(ser)
  }

  it should "derive generic type bounded classes" in {
    val ser = implicitly[TypeInformation[BoundADT[Foo]]].createSerializer(ec)
    noKryo(ser)
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
    serializable(ser)
  }

  it should "be serializable in case of annotations on subtypes" in {
    val ser = implicitly[TypeInformation[Ann]].createSerializer(ec)
    serializable(ser)
  }

  it should "serialize Option" in {
    val ser = implicitly[TypeInformation[SimpleOption]].createSerializer(ec)
    all(ser, SimpleOption(None))
    roundtrip(ser, SimpleOption(Some("foo")))
  }

  it should "serialize Either" in {
    val ser = implicitly[TypeInformation[SimpleEither]].createSerializer(ec)
    all(ser, SimpleEither(Left("foo")))
    roundtrip(ser, SimpleEither(Right(42)))
  }

  it should "serialize nested list of ADT" in {
    val ser = implicitly[TypeInformation[ListADT]].createSerializer(ec)
    all(ser, ListADT(Nil))
    roundtrip(ser, ListADT(List(Foo("a"))))
  }

  it should "derive multiple instances of generic class" in {
    val ser  = implicitly[TypeInformation[Generic[SimpleOption]]].createSerializer(ec)
    val ser2 = implicitly[TypeInformation[Generic[Simple]]].createSerializer(ec)
    all(ser, Generic(SimpleOption(None), Bar(0)))
    all(ser2, Generic(Simple(0, "asd"), Bar(0)))
  }

  it should "serialize nil" in {
    val ser = implicitly[TypeInformation[NonEmptyList[String]]].createSerializer(ec)
    roundtrip(ser, NonEmptyList.one("a"))
  }

  it should "serialize unit" in {
    val ser = implicitly[TypeInformation[Unit]].createSerializer(ec)
    roundtrip(ser, ())
  }

  it should "serialize triple-nested case clases" in {
    val ser = implicitly[TypeInformation[Seq[NestedBottom]]].createSerializer(ec)
    roundtrip(ser, List(NestedBottom(Some("a"), None)))
  }

  it should "serialize classes with type mapper" in {
    import MappedTypeInfoTest._
    val ser = implicitly[TypeInformation[WrappedString]].createSerializer(ec)
    val str = new WrappedString()
    str.put("foo")
    roundtrip(ser, str)
  }

  it should "serialize bigint" in {
    val ser = implicitly[TypeInformation[BigInt]].createSerializer(ec)
    roundtrip(ser, BigInt(123))
  }

  it should "serialize bigdec" in {
    val ser = implicitly[TypeInformation[BigDecimal]].createSerializer(ec)
    roundtrip(ser, BigDecimal(123))
  }

  it should "serialize uuid" in {
    val ser = implicitly[TypeInformation[UUID]].createSerializer(ec)
    roundtrip(ser, UUID.randomUUID())
  }

  it should "serialize case class with private field" in {
    val ser = implicitly[TypeInformation[PrivateField]].createSerializer(ec)
    roundtrip(ser, PrivateField("abc"))
  }

  it should "serialize a case class overriding a field" in {
    val ser = implicitly[TypeInformation[ExtendingCaseClass]].createSerializer(ec)
    roundtrip(ser, ExtendingCaseClass("abc", "def"))
  }

  it should "serialize a case class with nullable field" in {
    val ser = implicitly[TypeInformation[NullableField]].createSerializer(ec)
    roundtrip(ser, NullableField(null, Bar(1)))
  }

  it should "serialize a case class with a nullable field of a case class with no arity" in {
    val ser = implicitly[TypeInformation[NullableFieldWithNoArity]].createSerializer(ec)
    roundtrip(ser, NullableFieldWithNoArity(null))
  }

}

object SerializerTest {
  case class Simple(a: Int, b: String)
  case class SimpleList(a: List[Int])
  case class SimpleJava(a: Integer, b: String)
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

}
