package org.apache.flinkx.api

import cats.data.NonEmptyList
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flinkx.api.SerializerTest.DeeplyNested.ModeNested.SuperNested.Egg
import org.apache.flinkx.api.SerializerTest.NestedRoot.NestedMiddle.NestedBottom
import org.apache.flinkx.api.SerializerTest._
import org.apache.flinkx.api.serializer._
import org.apache.flinkx.api.serializers._
import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time._
import java.util.UUID
import scala.collection.immutable.{BitSet, SortedMap, SortedSet, TreeMap, TreeSet}
import scala.collection.mutable
import scala.concurrent.duration.Duration

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

  it should "serialize a Finite Duration" in {
    testTypeInfoAndSerializer(Duration.Zero, nullable = false)
  }

  it should "serialize an Undefined Duration" in {
    testTypeInfoAndSerializer[Duration](Duration.Undefined)
  }

  it should "serialize a positive Infinite Duration" in {
    testTypeInfoAndSerializer[Duration](Duration.Inf)
  }

  it should "serialize a negative Infinite Duration" in {
    testTypeInfoAndSerializer[Duration](Duration.MinusInf)
  }

  it should "serialize FiniteDuration" in {
    testTypeInfoAndSerializer(Duration.Zero, nullable = false)
  }

  it should "serialize org.apache.flink.types.Nothing" in {
    val info = implicitly[TypeInformation[org.apache.flink.types.Nothing]]
    testTypeInfo(info)
    val ser = infoToSer[org.apache.flink.types.Nothing](info)
    noKryo(ser)
    javaSerializable(ser)
  }

  it should "serialize scala.Nothing" in {
    val info = implicitly[TypeInformation[Nothing]]
    testTypeInfo(info)
    val ser = infoToSer[Nothing](info)
    noKryo(ser)
    javaSerializable(ser)
  }

  it should "serialize mutable.ArrayDeque" in {
    testTypeInfoAndSerializer(mutable.ArrayDeque("1", "2", "3"))
  }

  it should "serialize mutable.Buffer" in {
    testTypeInfoAndSerializer(mutable.Buffer("1", "2", "3"))
  }

  it should "serialize mutable.ArrayBuffer (default implementation of mutable.Buffer)" in {
    testTypeInfoAndSerializer(mutable.ArrayBuffer("1", "2", "3"))
  }

  it should "serialize mutable.ListBuffer (a different implementation than the mutable.Buffer default)" in {
    testTypeInfoAndSerializer[mutable.Buffer[String]](mutable.ListBuffer("1", "2", "3"))
  }

  it should "serialize mutable.Queue" in {
    testTypeInfoAndSerializer(mutable.Queue("1", "2", "3"))
  }

  it should "serialize mutable.Set" in {
    testTypeInfoAndSerializer(mutable.Set("1", "2", "3"))
  }

  it should "serialize mutable.HashSet (default implementation of mutable.Set)" in {
    testTypeInfoAndSerializer(mutable.HashSet("1", "2", "3"))
  }

  it should "serialize mutable.LinkedHashSet (a different implementation than the mutable.Set default)" in {
    testTypeInfoAndSerializer[mutable.Set[String]](mutable.LinkedHashSet("1", "2", "3"))
  }

  it should "serialize mutable.Map of Int" in {
    testTypeInfoAndSerializer(mutable.Map(1 -> 9, 2 -> 8, 3 -> 7))
  }

  it should "serialize mutable.Map" in {
    testTypeInfoAndSerializer(mutable.Map("1" -> "a", "2" -> "b", "3" -> "c"))
  }

  it should "serialize mutable.HashMap (default implementation of mutable.Map)" in {
    testTypeInfoAndSerializer(mutable.HashMap("1" -> "a", "2" -> "b", "3" -> "c"))
  }

  it should "serialize mutable.LinkedHashMap (a different implementation than the mutable.Map default)" in {
    testTypeInfoAndSerializer[mutable.Map[String, String]](mutable.LinkedHashMap("1" -> "a", "2" -> "b", "3" -> "c"))
  }

  it should "serialize SortedSet of Unit" in {
    testTypeInfoAndSerializer[SortedSet[Unit]](SortedSet((), ()))
  }

  it should "serialize SortedSet of Booleans" in {
    implicit val orderingInfo: TypeInformation[Ordering[Boolean]] = OrderingTypeInfo.DefaultBooleanOrderingInfo
    testTypeInfoAndSerializer(SortedSet(true, false))
  }

  it should "serialize SortedSet of Bytes" in {
    implicit val orderingInfo: TypeInformation[Ordering[Byte]] = OrderingTypeInfo.DefaultByteOrderingInfo
    testTypeInfoAndSerializer(SortedSet[Byte](3, 1, 2))
  }

  it should "serialize SortedSet of Chars" in {
    implicit val orderingInfo: TypeInformation[Ordering[Char]] = OrderingTypeInfo.DefaultCharOrderingInfo
    testTypeInfoAndSerializer(SortedSet[Char]('3', '1', '2'))
  }

  it should "serialize SortedSet of Shorts" in {
    implicit val orderingInfo: TypeInformation[Ordering[Short]] = OrderingTypeInfo.DefaultShortOrderingInfo
    testTypeInfoAndSerializer(SortedSet[Short](3, 1, 2))
  }

  it should "serialize SortedSet of Ints" in {
    implicit val orderingInfo: TypeInformation[Ordering[Int]] = OrderingTypeInfo.DefaultIntOrderingInfo
    testTypeInfoAndSerializer(SortedSet(3, 1, 2))
  }

  it should "serialize SortedSet of Longs" in {
    implicit val orderingInfo: TypeInformation[Ordering[Long]] = OrderingTypeInfo.DefaultLongOrderingInfo
    testTypeInfoAndSerializer(SortedSet(3L, 1L, 2L))
  }

  it should "serialize SortedSet of Floats" in {
    implicit val orderingInfo: TypeInformation[Ordering[Float]] = OrderingTypeInfo.DefaultFloatOrderingInfo
    testTypeInfoAndSerializer(SortedSet(3.0f, 1.0f, 2.0f))
  }

  it should "serialize SortedSet of Floats ordered by IeeeOrdering" in {
    implicit val ordering: Ordering[Float]                      = Ordering.Float.IeeeOrdering
    implicit val orderingInfo: TypeInformation[Ordering[Float]] = OrderingTypeInfo.FloatIeeeOrderingInfo
    testTypeInfoAndSerializer(SortedSet(3.0f, 1.0f, 2.0f))
  }

  it should "serialize SortedSet of Doubles" in {
    implicit val orderingInfo: TypeInformation[Ordering[Double]] = OrderingTypeInfo.DefaultDoubleOrderingInfo
    testTypeInfoAndSerializer(SortedSet(3.0, 1.0, 2.0))
  }

  it should "serialize SortedSet of Doubles ordered by IeeeOrdering" in {
    implicit val ordering: Ordering[Double]                      = Ordering.Double.IeeeOrdering
    implicit val orderingInfo: TypeInformation[Ordering[Double]] = OrderingTypeInfo.DoubleIeeeOrderingInfo
    testTypeInfoAndSerializer(SortedSet(3.0, 1.0, 2.0))
  }

  it should "serialize SortedSet of BigInts" in {
    implicit val orderingInfo: TypeInformation[Ordering[BigInt]] = OrderingTypeInfo.DefaultBigIntOrderingInfo
    testTypeInfoAndSerializer(SortedSet(BigInt(3), BigInt(1), BigInt(2)))
  }

  it should "serialize SortedSet of BigDecimals" in {
    implicit val orderingInfo: TypeInformation[Ordering[BigDecimal]] = OrderingTypeInfo.DefaultBigDecimalOrderingInfo
    testTypeInfoAndSerializer(SortedSet(BigDecimal(3.0), BigDecimal(1.0), BigDecimal(2.0)))
  }

  it should "serialize SortedSet of Strings" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    testTypeInfoAndSerializer(SortedSet("3", "1", "2"))
  }

  it should "serialize SortedSet of Strings ordered by reverse" in {
    implicit val ordering: Ordering[String]                      = Ordering.String.reverse
    implicit val orderingInfo: TypeInformation[Ordering[String]] =
      OrderingTypeInfo.reverse(OrderingTypeInfo.DefaultStringOrderingInfo)
    testTypeInfoAndSerializer(SortedSet("3", "1", "2"))
  }

  it should "serialize SortedSet of optional Strings" in {
    implicit val ordering: Ordering[Option[String]]                      = Ordering.Option(Ordering.String)
    implicit val orderingInfo: TypeInformation[Ordering[Option[String]]] =
      OrderingTypeInfo.Option(OrderingTypeInfo.DefaultStringOrderingInfo)
    testTypeInfoAndSerializer(SortedSet(Some("3"), None, Some("2")))
  }

  it should "serialize SortedSet with complex ordering" in {
    implicit val ordering: Ordering[Simple]                      = SimpleOrdering
    implicit val orderingInfo: TypeInformation[Ordering[Simple]] =
      OrderingTypeInfo.deriveOrdering[SimpleOrdering.type, Simple]
    testTypeInfoAndSerializer(SortedSet(Simple(3, "3"), Simple(1, "1"), Simple(2, "2")))
  }

  it should "serialize TreeSet of Strings (default implementation of SortedSet)" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    testTypeInfoAndSerializer(TreeSet("3", "1", "2"))
  }

  it should "serialize BitSet of Ints (a different implementation than the SortedSet default)" in {
    implicit val orderingInfo: TypeInformation[Ordering[Int]] = OrderingTypeInfo.DefaultIntOrderingInfo
    testTypeInfoAndSerializer[SortedSet[Int]](BitSet(3, 1, 2))
  }

  it should "serialize SortedMap" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    testTypeInfoAndSerializer(SortedMap("3" -> "c", "1" -> "a", "2" -> "b"))
  }

  it should "serialize TreeMap (default implementation of SortedMap)" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    testTypeInfoAndSerializer(TreeMap("3" -> "c", "1" -> "a", "2" -> "b"))
  }

  it should "serialize SortedMap.WithDefault (a different implementation than the SortedMap default)" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    // Works but WithDefault behavior is lost at deserialization
    testTypeInfoAndSerializer(SortedMap("3" -> "c", "1" -> "a", "2" -> "b").withDefaultValue("0"))
  }

  it should "serialize mutable.SortedMap" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    testTypeInfoAndSerializer(mutable.SortedMap("3" -> "c", "1" -> "a", "2" -> "b"))
  }

  it should "serialize mutable.TreeMap (default implementation of SortedMap)" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    testTypeInfoAndSerializer(mutable.TreeMap("3" -> "c", "1" -> "a", "2" -> "b"))
  }

  it should "serialize mutable.SortedMap.WithDefault (a different implementation than the SortedMap default)" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    // Works but WithDefault behavior is lost at deserialization
    testTypeInfoAndSerializer(mutable.SortedMap("3" -> "c", "1" -> "a", "2" -> "b").withDefaultValue("0"))
  }

  it should "serialize mutable.SortedSet" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    testTypeInfoAndSerializer(mutable.SortedSet("3", "1", "2"))
  }

  it should "serialize mutable.TreeSet (default implementation of SortedSet)" in {
    implicit val orderingInfo: TypeInformation[Ordering[String]] = OrderingTypeInfo.DefaultStringOrderingInfo
    testTypeInfoAndSerializer(mutable.TreeSet("3", "1", "2"))
  }

  it should "serialize mutable.BitSet (a different implementation than the SortedSet default)" in {
    implicit val orderingInfo: TypeInformation[Ordering[Int]] = OrderingTypeInfo.DefaultIntOrderingInfo
    testTypeInfoAndSerializer[mutable.SortedSet[Int]](mutable.BitSet(3, 1, 2))
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

  case object SimpleOrdering extends Ordering[Simple] {
    override def compare(x: Simple, y: Simple): Int = x.a.compare(y.a)
  }

}
