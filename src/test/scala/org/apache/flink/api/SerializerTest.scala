package org.apache.flink.api

import cats.data.NonEmptyList
import org.apache.flink.api.SerializerTest.DeeplyNested.ModeNested.SuperNested.{Egg, Food}
import org.apache.flink.api.SerializerTest.NestedRoot.NestedMiddle.NestedBottom

import org.apache.flink.api.SerializerTest.{
  ADT,
  ADT2,
  Ann,
  Annotated,
  Bar,
  Bar2,
  BoundADT,
  Foo,
  Foo2,
  Generic,
  ListADT,
  Nested,
  P2,
  Param,
  Simple,
  SimpleEither,
  SimpleJava,
  SimpleList,
  SimpleOption,
  SimpleSeq,
  SimpleSeqSeq,
  WrappedADT
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flink.api.serializers._

class SerializerTest extends AnyFlatSpec with Matchers with Inspectors with TestUtils {

  it should "derive serializer for simple class" in {
    val ser = implicitly[TypeInformation[Simple]].createSerializer(null)
    all(ser, Simple(1, "foo"))
  }

  it should "derive serializer for java classes" in {
    val ser = implicitly[TypeInformation[SimpleJava]].createSerializer(null)
    all(ser, SimpleJava(1, "foo"))
  }

  it should "derive nested classes" in {
    val ser = implicitly[TypeInformation[Nested]].createSerializer(null)
    all(ser, Nested(Simple(1, "foo")))
  }

  it should "derive for ADTs" in {
    val ser = implicitly[TypeInformation[ADT]].createSerializer(null)
    all(ser, Foo("a"))
    all(ser, Bar(1))
  }

  it should "derive for ADTs with case objects" in {
    val ser = implicitly[TypeInformation[ADT2]].createSerializer(null)
    all(ser, Foo2)
    all(ser, Bar2)
  }

  it should "derive for deeply nested classes" in {
    val ser = implicitly[TypeInformation[Egg]].createSerializer(null)
    all(ser, Egg(1))
  }

  it should "derive for deeply nested adts" in {
    val ser = implicitly[TypeInformation[Food]].createSerializer(null)
    all(ser, Egg(1))
  }

  it should "derive for nested ADTs" in {
    val ser = implicitly[TypeInformation[WrappedADT]].createSerializer(null)
    all(ser, WrappedADT(Foo("a")))
    all(ser, WrappedADT(Bar(1)))
  }

  it should "derive for generic ADTs" in {
    val ser = implicitly[TypeInformation[Param[Int]]].createSerializer(null)
    all(ser, P2(1))
  }

  it should "derive seq" in {
    val ser = implicitly[TypeInformation[SimpleSeq]].createSerializer(null)
    noKryo(ser)
    serializable(ser)
  }

  it should "derive list of ADT" in {
    val ser = implicitly[TypeInformation[List[ADT]]].createSerializer(null)
    all(ser, List(Foo("a")))
    roundtrip(ser, ::(Foo("a"), Nil))
    roundtrip(ser, Nil)
  }

  it should "derive recursively" in {
    // recursive is broken
    // val ti = implicitly[TypeInformation[Node]]
  }

  it should "derive list" in {
    val ser = implicitly[TypeInformation[List[Simple]]].createSerializer(null)
    all(ser, List(Simple(1, "a")))
  }

  it should "derive nested list" in {
    val ser = implicitly[TypeInformation[List[SimpleList]]].createSerializer(null)
    all(ser, List(SimpleList(List(1))))
  }

  it should "derive seq of seq" in {
    val ser = implicitly[TypeInformation[SimpleSeqSeq]].createSerializer(null)
    noKryo(ser)
    serializable(ser)
  }

  it should "derive generic type bounded classes" in {
    val ser = implicitly[TypeInformation[BoundADT[Foo]]].createSerializer(null)
    noKryo(ser)
  }

//  it should "derive nested generic type bounded classes" in {
//    val ser = implicitly[TypeInformation[NestedParent]].createSerializer(null)
//    noKryo(ser)
//  }

  it should "be serializable in case of annotations on classes" in {
    val ser = implicitly[TypeInformation[Annotated]].createSerializer(null)
    serializable(ser)
  }

  it should "be serializable in case of annotations on subtypes" in {
    val ser = implicitly[TypeInformation[Ann]].createSerializer(null)
    serializable(ser)
  }

  it should "serialize Option" in {
    val ser = implicitly[TypeInformation[SimpleOption]].createSerializer(null)
    all(ser, SimpleOption(None))
    roundtrip(ser, SimpleOption(Some("foo")))
  }

  it should "serialize Either" in {
    val ser = implicitly[TypeInformation[SimpleEither]].createSerializer(null)
    all(ser, SimpleEither(Left("foo")))
    roundtrip(ser, SimpleEither(Right(42)))
  }

  it should "serialize nested list of ADT" in {
    val ser = implicitly[TypeInformation[ListADT]].createSerializer(null)
    all(ser, ListADT(Nil))
    roundtrip(ser, ListADT(List(Foo("a"))))
  }

  it should "derive multiple instances of generic class" in {
    val ser  = implicitly[TypeInformation[Generic[SimpleOption]]].createSerializer(null)
    val ser2 = implicitly[TypeInformation[Generic[Simple]]].createSerializer(null)
    all(ser, Generic(SimpleOption(None), Bar(0)))
    all(ser2, Generic(Simple(0, "asd"), Bar(0)))
  }

  it should "serialize nil" in {
    val ser = implicitly[TypeInformation[NonEmptyList[String]]].createSerializer(null)
    roundtrip(ser, NonEmptyList.one("a"))
  }

  it should "serialize unit" in {
    val ser = implicitly[TypeInformation[Unit]].createSerializer(null)
    roundtrip(ser, ())
  }

  it should "serialize triple-nested case clases" in {
    val ser = implicitly[TypeInformation[Seq[NestedBottom]]].createSerializer(null)
    roundtrip(ser, List(NestedBottom(Some("a"), None)))
  }

  it should "serialize classes with type mapper" in {
    import MappedTypeInfoTest._
    val ser = implicitly[TypeInformation[WrappedString]].createSerializer(null)
    val str = new WrappedString()
    str.put("foo")
    roundtrip(ser, str)
  }

  it should "serialize bigint" in {
    val ser = implicitly[TypeInformation[BigInt]].createSerializer(null)
    roundtrip(ser, BigInt(123))
  }

  it should "serialize bigdec" in {
    val ser = implicitly[TypeInformation[BigDecimal]].createSerializer(null)
    roundtrip(ser, BigDecimal(123))
  }

}

object SerializerTest {
  case class Simple(a: Int, b: String)
  case class SimpleList(a: List[Int])
  case class SimpleJava(a: Integer, b: String)
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
}
