package org.apache.flinkx.api.rowdata

import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

class RowDataConverterTest extends AnyFlatSpec with Matchers {

  import RowDataConverterTest.*

  "RowDataConverter" should "derive for a single-field case class" in {
    val row = GenericRowData.of(StringData.fromString("hello"))

    row.toScala[Single] shouldBe Single("hello")
  }

  it should "map fields by declaration order" in {
    val row = GenericRowData.of(StringData.fromString("u1"), StringData.fromString("Alice"), Integer.valueOf(30))

    row.toScala[User] shouldBe User("u1", "Alice", 30)
  }

  it should "read every primitive type" in {
    val row = GenericRowData.of(
      java.lang.Boolean.TRUE,
      java.lang.Byte.valueOf(1.toByte),
      java.lang.Short.valueOf(2.toShort),
      Integer.valueOf(3),
      java.lang.Long.valueOf(4L),
      java.lang.Float.valueOf(5.0f),
      java.lang.Double.valueOf(6.0d)
    )

    row.toScala[Primitives] shouldBe Primitives(true, 1, 2, 3, 4L, 5.0f, 6.0d)
  }

  it should "round-trip case class -> RowData -> case class" in {
    val user = User("u1", "Alice", 30)

    user.toRowData.toScala[User] shouldBe user
  }

  it should "produce a RowData with one column per field" in {
    val row = User("u1", "Alice", 30).toRowData

    row.getArity shouldBe 3
    row.getString(0).toString shouldBe "u1"
    row.getInt(2) shouldBe 30
  }

  "a custom FieldConverter" should "override the built-in one for its type" in {
    val row = GenericRowData.of(StringData.fromString("u1"), java.lang.Long.valueOf(5000L))

    // EpochSeconds has its own given converter, dividing by 1000; the plain Long converter is not used
    row.toScala[Event] shouldBe Event("u1", EpochSeconds(5L))
  }

  it should "apply on the write path too" in {
    val row = Event("u1", EpochSeconds(5L)).toRowData

    row.getLong(1) shouldBe 5000L
  }

  it should "leave other fields of the same underlying type alone" in {
    // ts is an opaque Long with a custom converter, retryCount is a plain Long and is not affected
    val row = GenericRowData.of(java.lang.Long.valueOf(5000L), java.lang.Long.valueOf(7L))

    row.toScala[Retryable] shouldBe Retryable(EpochSeconds(5L), 7L)
  }

  "an Option field" should "read NULL as None" in {
    val row = GenericRowData.of(StringData.fromString("u1"), null)

    row.toScala[MaybeNamed] shouldBe MaybeNamed("u1", None)
  }

  it should "read a present value as Some" in {
    val row = GenericRowData.of(StringData.fromString("u1"), StringData.fromString("Alice"))

    row.toScala[MaybeNamed] shouldBe MaybeNamed("u1", Some("Alice"))
  }

  it should "write None back as NULL" in {
    val row = MaybeNamed("u1", None).toRowData

    row.isNullAt(1) shouldBe true
  }

  it should "round-trip both cases" in {
    val present = MaybeNamed("u1", Some("Alice"))
    val absent  = MaybeNamed("u1", None)

    present.toRowData.toScala[MaybeNamed] shouldBe present
    absent.toRowData.toScala[MaybeNamed] shouldBe absent
  }

  "a nested case class" should "read from a ROW column" in {
    val nested = GenericRowData.of(StringData.fromString("Berlin"), StringData.fromString("DE"))
    val row    = GenericRowData.of(StringData.fromString("u1"), nested)

    row.toScala[Person] shouldBe Person("u1", Address("Berlin", "DE"))
  }

  it should "round-trip" in {
    val person = Person("u1", Address("Berlin", "DE"))

    person.toRowData.toScala[Person] shouldBe person
  }

  "a derived converter" should "survive Java serialization" in {
    val converter = summon[RowDataConverter[User]]
    val user      = User("u1", "Alice", 30)

    roundTripJava(converter).fromRowData(user.toRowData) shouldBe user
  }

  "semiauto derivation" should "produce a working converter" in {
    import semiauto.*

    val converter = deriveRowDataConverter[User]
    val user      = User("u1", "Alice", 30)

    converter.fromRowData(converter.toRowData(user)) shouldBe user
  }

  "auto derivation" should "produce a converter without a derives clause" in {
    import auto.given

    val converter = summon[RowDataConverter[NoDerives]]
    val value     = NoDerives("u1", 30)

    converter.fromRowData(converter.toRowData(value)) shouldBe value
  }

  private def roundTripJava[A](value: A): A = {
    val bytes = new ByteArrayOutputStream()
    val out   = new ObjectOutputStream(bytes)
    out.writeObject(value)
    out.close()
    new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray)).readObject().asInstanceOf[A]
  }

}

object RowDataConverterTest {

  case class Single(value: String) derives RowDataConverter

  case class User(id: String, name: String, age: Int) derives RowDataConverter

  case class Primitives(a: Boolean, b: Byte, c: Short, d: Int, e: Long, f: Float, g: Double) derives RowDataConverter

  case class MaybeNamed(id: String, name: Option[String]) derives RowDataConverter

  case class Address(city: String, country: String) derives RowDataConverter

  case class Person(id: String, address: Address) derives RowDataConverter

  case class NoDerives(id: String, age: Int)

  export TimeTypes.EpochSeconds

  case class Event(userId: String, ts: EpochSeconds) derives RowDataConverter

  case class Retryable(ts: EpochSeconds, retryCount: Long) derives RowDataConverter

  object BigDecimalConverter {

    given FieldConverter[BigDecimal] = FieldConverter.decimal(precision = 5, scale = 2)

    case class Priced(amount: BigDecimal) derives RowDataConverter

  }

}

/** A distinct type for a field that needs conversion, so the custom converter applies to it alone.
  *
  * It has to live outside the scope that declares the case classes: inside its own defining scope an `opaque type` is
  * transparent, so the derivation there would see plain `Long` and pick the built-in converter.
  */
object TimeTypes {

  opaque type EpochSeconds = Long

  object EpochSeconds {
    def apply(value: Long): EpochSeconds = value

    given FieldConverter[EpochSeconds] with {
      def fromRowData(row: RowData, index: Int): EpochSeconds = row.getLong(index) / 1000
      def toRowData(value: EpochSeconds): AnyRef              = java.lang.Long.valueOf(value * 1000)
    }
  }

}
