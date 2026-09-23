package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.serializer.Scala3EnumValueSerializerSnapshotTest.Failure
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Scala3EnumValueSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "be compatible as is when the same enum value is serialized" in {
    val registeredSnapshot = enumValueSerializer("MISSING_KEY").snapshotConfiguration()
    val restoredSnapshot   = savepointed(enumValueSerializer("MISSING_KEY"))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when another enum value is serialized" in {
    val registeredSnapshot = enumValueSerializer("MISSING_KEY").snapshotConfiguration()
    val restoredSnapshot   = savepointed(enumValueSerializer("PARSE_ERROR"))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  /** Writes the snapshot of the serializer then reads it back, the way a savepoint does. */
  private def savepointed(serializer: TypeSerializer[Failure]): TypeSerializerSnapshot[Failure] = {
    val out = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration())
    val in = new DataInputDeserializer(out.getSharedBuffer)
    TypeSerializerSnapshot.readVersionedSnapshot[Failure](in, getClass.getClassLoader)
  }

  private def enumValueSerializer(enumValueName: String) =
    new Scala3EnumValueSerializer[Failure](Failure.getClass, enumValueName)

}

object Scala3EnumValueSerializerSnapshotTest {

  enum Failure {
    case MISSING_KEY, PARSE_ERROR
  }

}
