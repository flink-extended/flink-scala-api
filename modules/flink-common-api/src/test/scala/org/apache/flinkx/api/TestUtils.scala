package org.apache.flinkx.api

import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.core.memory._
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, MappedTypeInformation}
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{Assertion, Inspectors}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}
import java.lang.reflect.{Field, Modifier}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.annotation.tailrec
import scala.collection.immutable.TreeSet
import scala.util.Try

trait TestUtils extends Matchers with Inspectors {

  private val sc = new SerializerConfigImpl()

  /** Serializes and deserializes the given object using the provided serializer, then asserts that the result matches
    * the expected value.
    * @param ser
    *   The serializer to test.
    * @param expected
    *   The expected value after serialization and deserialization.
    * @param assertion
    *   A function to assert the result (first param) against the expected value (second param).
    * @tparam T
    *   The type of the object being serialized.
    */
  def roundtrip[T](
      ser: TypeSerializer[T],
      expected: T,
      assertion: (T, T) => Assertion = (_: T) shouldBe (_: T)
  ): Assertion = {
    val out = new ByteArrayOutputStream()
    ser.serialize(expected, new DataOutputViewStreamWrapper(out))
    val snapBytes = new ByteArrayOutputStream()
    TypeSerializerSnapshot.writeVersionedSnapshot(
      new DataOutputViewStreamWrapper(snapBytes),
      ser.snapshotConfiguration()
    )
    val restoredSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[T](
      new DataInputViewStreamWrapper(new ByteArrayInputStream(snapBytes.toByteArray)),
      ser.getClass.getClassLoader
    )
    val restoredSerializer = restoredSnapshot.restoreSerializer()
    val result             =
      restoredSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(out.toByteArray)))
    assertion(result, expected)
  }

  /** Checks the instance copy functionality.
    * @param ser
    *   The serializer to test.
    * @param expected
    *   The input object to be copied.
    * @param assertion
    *   A function to assert the copied instance (first parameter) against the expected original instance (second
    *   parameter).
    * @tparam T
    *   The type of the object being processed.
    */
  def checkInstanceCopy[T](
      ser: TypeSerializer[T],
      expected: T,
      assertion: (T, T) => Assertion = (_: T) shouldBe (_: T)
  ): Assertion = {
    val result = ser.copy(expected)
    assertion(result, expected)
  }

  /** Checks the binary direct-memory copy functionality.
    * @param ser
    *   The serializer to test.
    * @param expected
    *   The object to be serialized, copied, and deserialized.
    * @param assertion
    *   A function to assert the copied and deserialized object (first parameter) against the expected original object
    *   (second parameter).
    * @tparam T
    *   The type of the object being processed.
    */
  def checkBinaryCopy[T](
      ser: TypeSerializer[T],
      expected: T,
      assertion: (T, T) => Assertion = (_: T) shouldBe (_: T)
  ): Assertion = {
    val out = new DataOutputSerializer(1024 * 1024)
    ser.serialize(expected, out)
    val inCopy  = new DataInputDeserializer(out.getSharedBuffer)
    val outCopy = new DataOutputSerializer(1024 * 1024)
    ser.copy(inCopy, outCopy)
    val in     = new DataInputDeserializer(outCopy.getSharedBuffer)
    val result = ser.deserialize(in)
    assertion(result, expected)
  }

  def noKryo[T](ser: TypeSerializer[T]): Unit =
    ser match {
      case p: CaseClassSerializer[_] =>
        forAll(p.getFieldSerializers) { param =>
          noKryo(param)
        }
      case _: KryoSerializer[_] =>
        throw new IllegalArgumentException("kryo detected")
      case _ => // ok
    }

  def javaSerializable[T](ser: TypeSerializer[T]): Unit = {
    val stream = new ObjectOutputStream(new ByteArrayOutputStream())
    stream.writeObject(ser)
  }

  private def isBasicType[T](typeClass: Class[T]) = BasicTypeInfo.getInfoFor(typeClass) != null

  def checkBasicType[T](info: TypeInformation[T]): Assertion =
    withClue("isBasicType:")(info.isBasicType shouldBe isBasicType(info.getTypeClass))

  def checkTupleType[T](info: TypeInformation[T]): Assertion =
    withClue("isTupleType:")(info.isTupleType shouldBe classOf[TupleTypeInfoBase[_]].isAssignableFrom(info.getClass))

  private def isInstanceField(field: Field): Boolean = !Modifier.isStatic(field.getModifiers)

  private def isNotBitmapField(field: Field): Boolean = !field.getName.startsWith("bitmap$")

  @tailrec
  private def getAllFields[T](typeClass: Class[T], fields: Array[Field] = Array.empty): Array[Field] = {
    val allFields  = fields ++ typeClass.getDeclaredFields.filter(isInstanceField).filter(isNotBitmapField)
    val superClass = typeClass.getSuperclass
    if (superClass == null || superClass == classOf[Object]) {
      allFields
    } else {
      getAllFields(superClass, allFields)
    }
  }

  def checkArity[T](info: TypeInformation[T]): Assertion = {
    val typeClass = info.getTypeClass
    val arity     = typeClass match {
      case c if isBasicType(c) => 1 // Basic type has an arity 1
      case c if c.isEnum       => 1 // Considered as a basic type of arity 1
      case EnumerationClass    => 1 // Considered as a basic type of arity 1
      case _                   => getAllFields(typeClass).length
    }
    withClue("getArity:")(info.getArity shouldBe arity)
  }

  def getTotalFields[T](typeClass: Class[T]): Int = {
    typeClass match {
      case c if isBasicType(c) => 1 // Basic type has an arity 1
      case c if c.isEnum       => 1 // Considered as a basic type with 1 field
      case EnumerationClass    => 1 // Considered as a basic type with 1 field
      case LocalDateClass      => 1 // Considered as a basic type with 1 field
      case LocalTimeClass      => 1 // Considered as a basic type with 1 field
      case LocalDateTimeClass  => 1 // Considered as a basic type with 1 field
      case ThrowableClass      => 6 // Throwable has a recursive "cause" field
      case TreeSetClass        => 2 // TreeSet has a recursive structure that leads to infinite recursion
      case _                   =>
        val fields = getAllFields(typeClass)
        fields
          .foldLeft(0)((acc, field) => acc + getTotalFields(field.getType))
          .max(1) // The total number of fields must be at least 1.
    }
  }

  def checkTotalFields[T](info: TypeInformation[T]): Assertion = {
    val typeClass   = info.getTypeClass
    val totalFields = getTotalFields(typeClass)
    withClue("getTotalFields:")(info.getTotalFields shouldBe totalFields)
  }

  def checkKeyType[T](info: TypeInformation[T]): Assertion = info match {
    case p: CaseClassTypeInfo[_] =>
      withClue("isKeyType:")(info.isKeyType shouldBe p.getFieldTypes.forall(_.isKeyType))
    case _ => withClue("isKeyType:")(info.isKeyType shouldBe classOf[Comparable[_]].isAssignableFrom(info.getTypeClass))
  }

  def checkToString[T](info: TypeInformation[T]): Assertion =
    withClue("checkToString:")(
      info.toString shouldNot be(s"${info.getClass.getName}@${Integer.toHexString(System.identityHashCode(info))}")
    )

  def checkEquals[T](info: TypeInformation[T]): Assertion = withClue("equals:")(info.equals(info) shouldBe true)

  def checkHashCode[T](info: TypeInformation[T]): Assertion =
    withClue("hashCode:")(info.hashCode shouldBe info.hashCode)

  def checkCanEqual[T](info: TypeInformation[T]): Assertion = withClue("canEqual:")(info.canEqual(info) shouldBe true)

  /** Creates a Scalatest matcher that runs a series of checks on the provided TypeInformation instance to ensure it
    * behaves as expected.
    *
    * Enables the following syntax: {{{result should haveTypeInfo[T]}}}
    * @tparam T
    *   The type of the object to be tested for type information.
    * @return
    *   A `Matcher` that checks if the type information is correct for the given object.
    */
  def haveTypeInfo[T](implicit info: TypeInformation[T]): Matcher[T] =
    (_: T) => {
      val infoToCheck = info match {
        case mti: MappedTypeInformation[_, _] => mti.nested
        case _                                => info
      }
      val result = Try {
        checkBasicType(infoToCheck)
        checkTupleType(infoToCheck)
        //    checkArity(infoToCheck)
        //    checkTotalFields(infoToCheck)
        checkKeyType(infoToCheck)
        checkToString(infoToCheck)
        checkEquals(infoToCheck)
        checkHashCode(infoToCheck)
        checkCanEqual(infoToCheck)
      }
      MatchResult(
        result.isSuccess,
        s"failure on type info check: ${result.failed.getOrElse("")}",
        "type info is correct"
      )
    }

  /** Creates a Scalatest matcher that verifies if a given object supports a serialization roundtrip using the provided
    * `TypeSerializer`.
    *
    * Enables the following syntax: {{{result should beSerializable[T](nullable = true)}}}
    * @param nullable
    *   Indicates whether the serializer should handle null values. Default is true.
    * @param assertion
    *   A function used to verify that the deserialized object matches the original serialized object. Default is
    *   equality check.
    * @tparam T
    *   The type of the object to be tested for serialization.
    * @return
    *   A `Matcher` that checks if serialization and deserialization succeed for the given object.
    */
  def beSerializable[T](
      nullable: Boolean = true,
      assertion: (T, T) => Assertion = (_: T) shouldBe (_: T)
  )(implicit ser: TypeSerializer[T]): Matcher[T] =
    (expected: T) => {
      val result = Try {
        roundtrip(ser, expected, assertion)
        checkInstanceCopy(ser, expected, assertion)
        checkBinaryCopy(ser, expected, assertion)
        noKryo(ser)
        javaSerializable(ser)
        if (nullable) {
          withClue("The serializer must handle null but didn't:") {
            NullableSerializer.checkIfNullSupported(ser) shouldBe true
          }
        }
      }
      MatchResult(result.isSuccess, s"serialization failed: ${result.failed.getOrElse("")}", "serialization succeeded")
    }

  /** Creates a Scalatest matcher that verifies if a given object supports a serialization roundtrip using the provided
    * `TypeSerializer`.
    *
    * Enables the following syntax: {{{result should beSerializable[T]}}}
    * @tparam T
    *   The type of the object to be tested for serialization.
    * @return
    *   A `Matcher` that checks if serialization and deserialization succeed for the given object.
    */
  def beSerializable[T: TypeSerializer]: Matcher[T] = beSerializable[T]()

  /** Creates a Scalatest matcher that verifies if a given object has valid type information and supports a
    * serialization roundtrip.
    *
    * Enables the following syntax: {{{result should haveTypeInfoAndBeSerializable[T](nullable = false)}}}
    * @param nullable
    *   Indicates whether the serializer should handle null values. Default is true.
    * @param assertion
    *   A function used to verify that the deserialized object matches the original serialized object. Default is
    *   equality check.
    * @tparam T
    *   The type of the object to be tested for type information and serialization.
    * @return
    *   A `Matcher` that checks if the object has valid type information and supports a serialization roundtrip.
    */
  def haveTypeInfoAndBeSerializable[T: TypeInformation: TypeSerializer](
      nullable: Boolean = true,
      assertion: (T, T) => Assertion = (_: T) shouldBe (_: T)
  ): Matcher[T] =
    haveTypeInfo.and(beSerializable[T](nullable, assertion))

  /** Creates a Scalatest matcher that verifies if a given object has valid type information and supports a
    * serialization roundtrip.
    *
    * Enables the following syntax: {{{result should haveTypeInfoAndBeSerializable[T]}}}
    * @tparam T
    *   The type of the object to be tested.
    * @return
    *   A `Matcher` that checks if the object has valid type information and supports a serialization roundtrip.
    */
  def haveTypeInfoAndBeSerializable[T: TypeInformation: TypeSerializer]: Matcher[T] = haveTypeInfoAndBeSerializable[T]()

  private val EnumerationClass   = classOf[Enumeration#Value]
  private val ThrowableClass     = classOf[Throwable]
  private val TreeSetClass       = classOf[TreeSet[_]]
  private val LocalDateClass     = classOf[LocalDate]
  private val LocalTimeClass     = classOf[LocalTime]
  private val LocalDateTimeClass = classOf[LocalDateTime]

}
