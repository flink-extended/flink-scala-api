package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.serializer.CoproductSerializerSnapshotTest.{ADT, Bar, Foo}
import org.apache.flinkx.api.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoproductSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "serialize v3 then deserialize v3" in {
    // Create SerializerSnapshot
    val subtypeClasses: Array[Class[?]]              = Array(classOf[Foo], classOf[Bar])
    val subtypeSerializers: Array[TypeSerializer[?]] = Array(
      implicitly[TypeSerializer[Foo]],
      implicitly[TypeSerializer[Bar]]
    )
    val serializerSnapshot: CoproductSerializer.CoproductSerializerSnapshot[ADT] =
      new CoproductSerializer.CoproductSerializerSnapshot(
        Some(new CoproductSerializer[ADT](subtypeClasses, subtypeSerializers))
      )

    val expectedSerializer = serializerSnapshot.restoreSerializer()

    // Serialize SerializerSnapshot
    val snapshotOutput = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(snapshotOutput, serializerSnapshot)
    val snapshotInput = new DataInputDeserializer(snapshotOutput.getSharedBuffer)

    // Deserialize SerializerSnapshot
    val deserializedSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[ADT](snapshotInput, getClass.getClassLoader)

    // Check the compatibility of the current snapshot with the restored snapshot
    val compatibility = serializerSnapshot.resolveSchemaCompatibility(deserializedSnapshot)
    compatibility.isIncompatible shouldBe false

    val deserializedSerializer = deserializedSnapshot.restoreSerializer()
    deserializedSerializer shouldNot be theSameInstanceAs expectedSerializer
    deserializedSerializer should be(expectedSerializer)
  }

  it should "serialize v2 then deserialize v3" in {
    val subtypeClasses: Array[Class[?]]              = Array(classOf[Foo], classOf[Bar])
    val subtypeSerializers: Array[TypeSerializer[?]] = Array(
      implicitly[TypeSerializer[Foo]],
      implicitly[TypeSerializer[Bar]]
    )

    // Write a v2 snapshot payload (as CoproductSerializerSnapshot#writeSnapshot did when v2 was current)
    val out = new DataOutputSerializer(1024 * 1024)
    out.writeInt(subtypeClasses.length)
    subtypeClasses.foreach(c => out.writeUTF(c.getName))
    subtypeSerializers.foreach(s => TypeSerializerSnapshot.writeVersionedSnapshot(out, s.snapshotConfiguration()))

    // Restore the v2 snapshot as Flink would: no-arg constructor then readSnapshot with the persisted version
    val oldSnapshot = new CoproductSerializer.CoproductSerializerSnapshot[ADT]()
    oldSnapshot.readSnapshot(2, new DataInputDeserializer(out.getSharedBuffer), getClass.getClassLoader)

    // Current (v3) snapshot for the same schema
    val newSnapshot = new CoproductSerializer.CoproductSerializerSnapshot[ADT](
      Some(new CoproductSerializer[ADT](subtypeClasses, subtypeSerializers))
    )

    val compatibility = newSnapshot.resolveSchemaCompatibility(oldSnapshot)
    compatibility.isIncompatible shouldBe false
  }

}

object CoproductSerializerSnapshotTest {
  sealed trait ADT
  case class Foo(a: String) extends ADT
  case class Bar(b: Int)    extends ADT
}
