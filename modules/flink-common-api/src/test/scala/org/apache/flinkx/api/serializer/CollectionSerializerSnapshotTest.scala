package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.semiauto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollectionSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "serialize then deserialize" in {
    // Create SerializerSnapshot
    val tSerializer = implicitly[TypeSerializer[String]]
    val serializerSnapshot: CollectionSerializerSnapshot[Set, String, SetSerializer[String]] =
      new CollectionSerializerSnapshot(tSerializer, classOf[SetSerializer[String]], classOf[String])

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

}
