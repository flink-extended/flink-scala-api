package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.serializer.SnapshotTestUtil._
import org.apache.flinkx.api.semiauto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollectionSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "serialize then deserialize" in {
    // Create SerializerSnapshot
    val tSerializer = implicitly[TypeSerializer[String]]
    val serializerSnapshot: CollectionSerializerSnapshot[Set, String, SetSerializer[String]] =
      new CollectionSerializerSnapshot(
        tSerializer.snapshotConfiguration(),
        classOf[SetSerializer[String]],
        classOf[String]
      )

    val expectedSerializer = serializerSnapshot.restoreSerializer()

    // Serialize SerializerSnapshot
    val snapshotOutput = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(snapshotOutput, serializerSnapshot)
    val snapshotInput = new DataInputDeserializer(snapshotOutput.getSharedBuffer)

    // Deserialize SerializerSnapshot
    val deserializedSnapshot = TypeSerializerSnapshot
      .readVersionedSnapshot[SetSerializer[String]](snapshotInput, getClass.getClassLoader)

    val deserializedSerializer = deserializedSnapshot.restoreSerializer()
    deserializedSerializer shouldNot be theSameInstanceAs expectedSerializer
    deserializedSerializer should be(expectedSerializer)
  }

  it should "be compatible as is when the serializer of the elements is compatible" in {
    val registeredSnapshot = new SetSerializer[Foo](fooSerializer, classOf[Foo]).snapshotConfiguration()
    val restoredSnapshot   = savepointed[Set[Foo]](new SetSerializer[Foo](fooSerializer, classOf[Foo]))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when the serializer of the elements is incompatible" in {
    val registeredSnapshot = new SetSerializer[Foo](fooSerializer, classOf[Foo]).snapshotConfiguration()
    val restoredSnapshot   = savepointed[Set[Foo]](new SetSerializer[Bar](barSerializer, classOf[Bar]))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  it should "be incompatible when another collection is serialized" in {
    val registeredSnapshot = new SetSerializer[Foo](fooSerializer, classOf[Foo]).snapshotConfiguration()
    val restoredSnapshot   = savepointed[Set[Foo]](new ListSerializer[Foo](fooSerializer, classOf[Foo]))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  it should "be compatible with a reconfigured serializer when the serializer of the elements is reconfigured" in {
    val registeredSnapshot =
      new SetSerializer[String](new VersionedSerializer(1), classOf[String]).snapshotConfiguration()
    val restoredSnapshot =
      savepointed[Set[String]](new SetSerializer[String](new VersionedSerializer(0), classOf[String]))

    val compatibility = registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot)

    compatibility shouldBe Symbol("compatibleWithReconfiguredSerializer")
    elementSerializerOf(compatibility.getReconfiguredSerializer) should be(new VersionedSerializer(1))
  }

  private def elementSerializerOf(serializer: TypeSerializer[_]): TypeSerializer[_] =
    serializer
      .snapshotConfiguration()
      .asInstanceOf[CollectionSerializerSnapshot[Set, _, _]]
      .nestedSnapshot
      .restoreSerializer()

}
