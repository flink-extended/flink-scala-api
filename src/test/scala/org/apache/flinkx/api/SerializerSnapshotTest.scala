package org.apache.flinkx.api

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.flinkx.api.SerializerSnapshotTest.{
  ADT2,
  OuterTrait,
  SimpleClass1,
  SimpleClassArray,
  SimpleClassList,
  SimpleClassMap1,
  SimpleClassMap2,
  TraitMap
}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flinkx.api.serializers._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.ChildFirstClassLoader
import org.scalatest.Assertion

import java.net.URLClassLoader

class SerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "roundtrip product serializer snapshot" in {
    val ser = deriveTypeInformation[SimpleClass1].createSerializer(null)
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip coproduct serializer snapshot" in {
    val ser = deriveTypeInformation[OuterTrait].createSerializer(null)
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip coproduct serializer snapshot with singletons" in {
    val ser = deriveTypeInformation[ADT2].createSerializer(null)
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip serializer snapshot with list of primitives" in {
    val ser = deriveTypeInformation[List[Double]].createSerializer(null)
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip serializer snapshot with set as array of primitives" in {
    val ser = implicitly[TypeInformation[Set[Double]]].createSerializer(null)
    assertRoundtripSerializer(ser)
  }

  it should "do array ser snapshot" in {
    val set = deriveTypeInformation[SimpleClassArray].createSerializer(null)
    assertRoundtripSerializer(set)
  }

  it should "do map ser snapshot" in {
    assertRoundtripSerializer(deriveTypeInformation[SimpleClassMap1].createSerializer(null))
    assertRoundtripSerializer(deriveTypeInformation[SimpleClassMap2].createSerializer(null))
  }

  it should "do list ser snapshot" in {
    assertRoundtripSerializer(deriveTypeInformation[SimpleClassList].createSerializer(null))
  }

  it should "do map ser snapshot adt " in {
    implicit val ti: Typeclass[OuterTrait] = deriveTypeInformation[OuterTrait]
    drop(ti)
    assertRoundtripSerializer(deriveTypeInformation[TraitMap].createSerializer(null))
  }

  it should "be compatible after snapshot deserialization in different classloader" in {
    val ser = deriveTypeInformation[SimpleClass1].createSerializer(null)
    val cl  = newClassLoader(classOf[SimpleClass1])
    try {
      val restored      = roundtripSerializer(ser, cl)
      val compatibility = restored.snapshotConfiguration().resolveSchemaCompatibility(ser)
      compatibility shouldBe Symbol("compatibleAsIs")
    } finally {
      cl.close()
    }
  }

  def roundtripSerializer[T](ser: TypeSerializer[T], cl: ClassLoader = getClass.getClassLoader): TypeSerializer[T] = {
    val snap   = ser.snapshotConfiguration()
    val buffer = new ByteArrayOutputStream()
    val output = new DataOutputViewStreamWrapper(buffer)
    snap.writeSnapshot(output)
    output.close()
    val input = new DataInputViewStreamWrapper(new ByteArrayInputStream(buffer.toByteArray))
    snap.readSnapshot(ser.snapshotConfiguration().getCurrentVersion, input, cl)
    snap.restoreSerializer()
  }

  def assertRoundtripSerializer[T](ser: TypeSerializer[T]): Assertion = {
    val restored = roundtripSerializer(ser)
    ser shouldBe restored
  }

  def newClassLoader(cls: Class[_]): URLClassLoader = {
    val urls = Array(cls.getProtectionDomain.getCodeSource.getLocation)
    new ChildFirstClassLoader(urls, cls.getClassLoader, Array(), _ => {})
  }

}

object SerializerSnapshotTest {
  sealed trait OuterTrait
  case class SimpleClass1(a: String, b: Int)  extends OuterTrait
  case class SimpleClass2(a: String, b: Long) extends OuterTrait

  case class SimpleClassArray(a: Array[SimpleClass1])
  case class SimpleClassMap1(a: Map[String, SimpleClass1])
  case class SimpleClassMap2(a: Map[SimpleClass1, String])
  case class SimpleClassList(a: List[SimpleClass1])

  case class TraitMap(a: Map[OuterTrait, String])

  sealed trait ADT2
  case object Foo2 extends ADT2
  case object Bar2 extends ADT2

}
