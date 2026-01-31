package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.serializer.CoproductSerializerSnapshotTest.{ADT, Bar, Foo}
import org.apache.flinkx.api.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoproductSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "serialize then deserialize" in {
    // Create SerializerSnapshot
    val subtypeClasses: Array[Class[?]]              = Array(classOf[Foo], classOf[Bar])
    val subtypeSerializers: Array[TypeSerializer[?]] = Array(
      implicitly[TypeSerializer[Foo]],
      implicitly[TypeSerializer[Bar]]
    )
    val serializerSnapshot: CoproductSerializer.CoproductSerializerSnapshot[ADT] =
      new CoproductSerializer.CoproductSerializerSnapshot(subtypeClasses, subtypeSerializers)

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

object CoproductSerializerSnapshotTest {
  sealed trait ADT
  case class Foo(a: String) extends ADT
  case class Bar(b: Int)    extends ADT
}
