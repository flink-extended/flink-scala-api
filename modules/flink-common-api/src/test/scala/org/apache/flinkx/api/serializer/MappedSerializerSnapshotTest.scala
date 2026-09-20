package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.mapper.BigDecMapper
import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper
import org.apache.flinkx.api.serializer.MappedSerializerSnapshotTest._
import org.apache.flinkx.api.serializer.SnapshotTestUtil._
import org.apache.flinkx.api.semiauto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.math.{BigDecimal => JBigDecimal}

class MappedSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "serialize then deserialize" in {
    // Create SerializerSnapshot
    val mapper      = new BigDecMapper()
    val tSerializer = implicitly[TypeSerializer[JBigDecimal]]
    val serializerSnapshot: MappedSerializer.MappedSerializerSnapshot[scala.BigDecimal, JBigDecimal] =
      new MappedSerializer.MappedSerializerSnapshot(mapper, tSerializer.snapshotConfiguration())

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
    deserializedSnapshot.snapshot.restoreSerializer() should be(serializerSnapshot.snapshot.restoreSerializer())
  }

  it should "be compatible as is when the serializer of the mapped type is compatible" in {
    val registeredSnapshot = MappedSerializer(new AMapper, stringSerializer).snapshotConfiguration()
    val restoredSnapshot   = savepointed[String](MappedSerializer(new AMapper, stringSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when the serializer of the mapped type is incompatible" in {
    val registeredSnapshot = MappedSerializer(new AMapper, stringSerializer).snapshotConfiguration()
    val restoredSnapshot   = savepointed[String](MappedSerializer(new AMapper, new VersionedSerializer(0)))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  it should "be incompatible when the data are mapped by another mapper" in {
    val registeredSnapshot = MappedSerializer(new AMapper, stringSerializer).snapshotConfiguration()
    val restoredSnapshot   = savepointed[String](MappedSerializer(new BMapper, stringSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

}

object MappedSerializerSnapshotTest {

  class AMapper extends TypeMapper[String, String] {
    override def map(a: String): String       = a
    override def contramap(b: String): String = b
  }

  class BMapper extends TypeMapper[String, String] {
    override def map(a: String): String       = a
    override def contramap(b: String): String = b
  }

}
