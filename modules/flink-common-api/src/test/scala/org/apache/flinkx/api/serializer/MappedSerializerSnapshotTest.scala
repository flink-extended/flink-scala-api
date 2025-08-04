package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.mapper.BigDecMapper
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.math.{BigDecimal => JBigDecimal}

class MappedSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "serialize then deserialize" in {
    // Create SerializerSnapshot
    val mapper      = new BigDecMapper()
    val tSerializer = implicitly[TypeSerializer[JBigDecimal]]
    val serializerSnapshot: MappedSerializer.MappedSerializerSnapshot[scala.BigDecimal, JBigDecimal] =
      new MappedSerializer.MappedSerializerSnapshot(mapper, tSerializer)

    val expectedSerializer = serializerSnapshot.restoreSerializer()

    // Serialize SerializerSnapshot
    val snapshotOutput = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(snapshotOutput, serializerSnapshot)
    val snapshotInput = new DataInputDeserializer(snapshotOutput.getSharedBuffer)

    // Deserialize SerializerSnapshot
    val deserializedSnapshot = TypeSerializerSnapshot
      .readVersionedSnapshot[SetSerializer[String]](snapshotInput, getClass.getClassLoader)
      .asInstanceOf[MappedSerializer.MappedSerializerSnapshot[scala.BigDecimal, JBigDecimal]]

    deserializedSnapshot.restoreSerializer() shouldNot be theSameInstanceAs expectedSerializer
    deserializedSnapshot.ser should be(serializerSnapshot.ser)
  }

}
