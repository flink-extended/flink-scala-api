package org.apache.flinkx.api

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory._
import org.apache.flink.util.ChildFirstClassLoader
import org.apache.flinkx.api.SerializerSnapshotTest._
import org.apache.flinkx.api.serializers._
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URLClassLoader
import java.nio.file.{Files, Paths}
import java.util.UUID

class SerializerSnapshotTest extends AnyFlatSpec with Matchers {

  def createSerializer[T: TypeInformation] =
    implicitly[TypeInformation[T]].createSerializer(new ExecutionConfig())

  it should "roundtrip product serializer snapshot" in {
    val ser = createSerializer[SimpleClass1]
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip coproduct serializer snapshot" in {
    val ser = createSerializer[OuterTrait]
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip coproduct serializer snapshot with singletons" in {
    val ser = createSerializer[ADT2]
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip serializer snapshot with list of primitives" in {
    val ser = createSerializer[List[Double]]
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip serializer snapshot with set as array of primitives" in {
    val ser = createSerializer[Set[Double]]
    assertRoundtripSerializer(ser)
  }

  it should "do array ser snapshot" in {
    val set = createSerializer[SimpleClassArray]
    assertRoundtripSerializer(set)
  }

  it should "do map ser snapshot" in {
    assertRoundtripSerializer(createSerializer[SimpleClassMap1])
    assertRoundtripSerializer(createSerializer[SimpleClassMap2])
  }

  it should "do list ser snapshot" in {
    assertRoundtripSerializer(createSerializer[SimpleClassList])
  }

  it should "do map ser snapshot adt " in {
    implicit val ti: Typeclass[OuterTrait] = deriveTypeInformation[OuterTrait]
    drop(ti)
    assertRoundtripSerializer(createSerializer[TraitMap])
  }

  it should "be compatible after snapshot deserialization in different classloader" in {
    val ser = createSerializer[SimpleClass1]
    val cl  = newClassLoader(classOf[SimpleClass1])
    try {
      val restored      = roundtripSerializer(ser, cl)
      val compatibility = restored.snapshotConfiguration().resolveSchemaCompatibility(ser)
      compatibility shouldBe Symbol("compatibleAsIs")
    } finally {
      cl.close()
    }
  }

  ignore should "serialize old serialization with the old serializer snapshot" in {
    val uuid                 = UUID.fromString("4daf2791-abbe-420f-9594-f57ded1fee8c")
    val expectedData         = OuterClass(Map(uuid -> List(SimpleClass2("a", 1))))
    val outerClassSerializer = implicitly[TypeSerializer[OuterClass]]
    val oldSnapshot          = outerClassSerializer.snapshotConfiguration()

    // Serialize the old snapshot
    val out = new DataOutputSerializer(1483)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, oldSnapshot) // Flink always calls this

    // Serialize the data
    outerClassSerializer.serialize(expectedData, out)
    Files.write(Paths.get("modules/flink-1-api/src/test/resources/old-serializer-snapshot.dat"), out.getSharedBuffer)
  }

  it should "deserialize old serialization with the new serializer snapshot" in {
    val uuid         = UUID.fromString("4daf2791-abbe-420f-9594-f57ded1fee8c")
    val expectedData = OuterClass(Map(uuid -> List(SimpleClass2("a", 1))))

    // Deserialize the old serialization
    val buffer = getClass.getResourceAsStream("/old-serializer-snapshot.dat").readAllBytes()
    val in     = new DataInputDeserializer(buffer)
    val deserializedSnapshot = TypeSerializerSnapshot
      .readVersionedSnapshot[OuterClass](in, getClass.getClassLoader) // Flink always calls this
    val deserializedSerializer = deserializedSnapshot.restoreSerializer()

    // Deserialize the data
    val deserializedData = deserializedSerializer.deserialize(in)
    deserializedData should be(expectedData)
  }

  it should "serialize and deserialize with the new snapshot" in {
    val uuid                 = UUID.randomUUID()
    val expectedData         = OuterClass(Map(uuid -> List(SimpleClass2("a", 1))))
    val outerClassSerializer = implicitly[TypeSerializer[OuterClass]]
    val oldSnapshot          = outerClassSerializer.snapshotConfiguration()

    // Serialize the new snapshot
    val oldOutput = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(oldOutput, oldSnapshot) // Flink always calls this

    // Serialize the data
    outerClassSerializer.serialize(expectedData, oldOutput)

    // Deserialize the new snapshot
    val oldInput = new DataInputDeserializer(oldOutput.getSharedBuffer)
    val deserializedOldSnapshot = TypeSerializerSnapshot
      .readVersionedSnapshot[OuterClass](oldInput, getClass.getClassLoader) // Flink always calls this
    val deserializedSerializer = deserializedOldSnapshot.restoreSerializer()

    // Deserialize the data
    val deserializedData = deserializedSerializer.deserialize(oldInput)
    deserializedData should be(expectedData)
  }

  def roundtripSerializer[T](ser: TypeSerializer[T], cl: ClassLoader = getClass.getClassLoader): TypeSerializer[T] = {
    val snap   = ser.snapshotConfiguration()
    val buffer = new ByteArrayOutputStream()
    val output = new DataOutputViewStreamWrapper(buffer)
    TypeSerializerSnapshot.writeVersionedSnapshot(output, snap)
    output.close()
    val input     = new DataInputViewStreamWrapper(new ByteArrayInputStream(buffer.toByteArray))
    val deserSnap = TypeSerializerSnapshot.readVersionedSnapshot[T](input, cl)
    deserSnap.restoreSerializer()
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

  case class OuterClass(map: Map[UUID, List[OuterTrait]])

}
