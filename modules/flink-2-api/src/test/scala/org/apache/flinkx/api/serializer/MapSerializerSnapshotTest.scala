package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataInputView, DataOutputSerializer, DataOutputView}
import org.apache.flinkx.api.serializer.MapSerializer.MapSerializerSnapshot
import org.apache.flinkx.api.serializer.MapSerializerSnapshotTest.*
import org.apache.flinkx.api.serializers.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MapSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "serialize the old data and deserialize them to the new data" in {
    // Old data of the type Map[String, OldClass]
    MapSerializerSnapshotTest.VersionOfTheCode = 0
    val oldData = Map("1" -> OldClass(1))

    val keySerializer      = implicitly[TypeSerializer[String]]
    val oldValueSerializer = new OldOuterTraitSerializer()

    // Create MapSerializerSnapshot
    val mapSerializerSnapshot: MapSerializerSnapshot[String, OuterTrait] =
      MapSerializerSnapshot(keySerializer, oldValueSerializer)

    val mapSerializer: TypeSerializer[Map[String, OuterTrait]] = mapSerializerSnapshot.restoreSerializer()

    // Serialize MapSerializerSnapshot
    val oldOutput = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(oldOutput, mapSerializerSnapshot)

    // Serialize the old data
    mapSerializer.serialize(oldData, oldOutput)

    // Switch to a new version of the code, now the data are expected to be of the type Map[String, NewClass]
    MapSerializerSnapshotTest.VersionOfTheCode = 1

    val oldInput = new DataInputDeserializer(oldOutput.getSharedBuffer)

    // Deserialize MapSerializerSnapshot
    val reconfiguredSnapshot: TypeSerializerSnapshot[Map[String, OuterTrait]] = TypeSerializerSnapshot
      .readVersionedSnapshot[Map[String, OuterTrait]](oldInput, getClass.getClassLoader)
    val reconfiguredMapSnapshot = reconfiguredSnapshot.asInstanceOf[MapSerializerSnapshot[String, OuterTrait]]
    reconfiguredMapSnapshot.valueSerializer should be(a[ReconfiguredOuterTraitSerializer])
    val reconfiguredSerializer = reconfiguredSnapshot.restoreSerializer()

    // Deserialize the old data but convert them to the new data
    val reconfiguredData = reconfiguredSerializer.deserialize(oldInput)
    reconfiguredData should be(Map("1" -> NewClass(1L)))

    // Serialize MapSerializerSnapshot
    val newOutput = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(newOutput, reconfiguredSnapshot)

    // Serialize the new data
    reconfiguredSerializer.serialize(reconfiguredData, newOutput)

    val newInput = new DataInputDeserializer(newOutput.getSharedBuffer)

    // Deserialize MapSerializerSnapshot
    val newSnapshot: TypeSerializerSnapshot[Map[String, OuterTrait]] = TypeSerializerSnapshot
      .readVersionedSnapshot[Map[String, OuterTrait]](newInput, getClass.getClassLoader)
    val newMapSnapshot = newSnapshot.asInstanceOf[MapSerializerSnapshot[String, OuterTrait]]
    newMapSnapshot.valueSerializer should be(a[NewOuterTraitSerializer])

    val newSerializer = newSnapshot.restoreSerializer()

    // Deserialize the new data
    val newData = newSerializer.deserialize(newInput)
    newData should be(Map("1" -> NewClass(1L)))
  }

}

object MapSerializerSnapshotTest {

  var VersionOfTheCode = 0

  sealed trait OuterTrait
  case class OldClass(a: Int)  extends OuterTrait
  case class NewClass(a: Long) extends OuterTrait

  class OldOuterTraitSerializer extends ImmutableSerializer[OuterTrait] {

    private val intSer = implicitly[TypeSerializer[Int]]

    override def createInstance(): OuterTrait = OldClass(0)

    override def getLength: Int = 4

    override def serialize(record: OuterTrait, target: DataOutputView): Unit = {
      val oc = record.asInstanceOf[OldClass]
      intSer.serialize(oc.a, target)
    }

    override def deserialize(source: DataInputView): OuterTrait = {
      val a = intSer.deserialize(source)
      OldClass(a)
    }

    override def snapshotConfiguration(): TypeSerializerSnapshot[OuterTrait] = new OuterTraitSerializerSnapshot()

  }

  class ReconfiguredOuterTraitSerializer extends ImmutableSerializer[OuterTrait] {

    private val intSer  = implicitly[TypeSerializer[Int]]
    private val longSer = implicitly[TypeSerializer[Long]]

    override def createInstance(): OuterTrait = NewClass(0L)

    override def getLength: Int = -1

    override def serialize(record: OuterTrait, target: DataOutputView): Unit = {
      val nc = record.asInstanceOf[NewClass]
      longSer.serialize(nc.a, target)
    }

    override def deserialize(source: DataInputView): OuterTrait = {
      val a = intSer.deserialize(source)
      NewClass(a.toLong)
    }

    override def snapshotConfiguration(): TypeSerializerSnapshot[OuterTrait] = new OuterTraitSerializerSnapshot()

  }

  class NewOuterTraitSerializer extends ImmutableSerializer[OuterTrait] {

    private val longSer = implicitly[TypeSerializer[Long]]

    override def createInstance(): OuterTrait = NewClass(0L)

    override def getLength: Int = 8

    override def serialize(record: OuterTrait, target: DataOutputView): Unit = {
      val nc = record.asInstanceOf[NewClass]
      longSer.serialize(nc.a, target)
    }

    override def deserialize(source: DataInputView): OuterTrait = {
      val a = longSer.deserialize(source)
      NewClass(a)
    }

    override def snapshotConfiguration(): TypeSerializerSnapshot[OuterTrait] = new OuterTraitSerializerSnapshot()

  }

  class OuterTraitSerializerSnapshot extends TypeSerializerSnapshot[OuterTrait] {

    private var ser: TypeSerializer[OuterTrait] = new OldOuterTraitSerializer()

    override def getCurrentVersion: Int = VersionOfTheCode

    override def writeSnapshot(out: DataOutputView): Unit = {}

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
      if (readVersion == 0) {
        ser = new ReconfiguredOuterTraitSerializer()
      } else {
        ser = new NewOuterTraitSerializer()
      }
    }

    override def restoreSerializer(): TypeSerializer[OuterTrait] = ser

    override def resolveSchemaCompatibility(
        oldSerializerSnapshot: TypeSerializerSnapshot[OuterTrait]
    ): TypeSerializerSchemaCompatibility[OuterTrait] = {
      if (oldSerializerSnapshot.getCurrentVersion == 0) {
        TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(new ReconfiguredOuterTraitSerializer())
      } else {
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      }
    }

  }

}
