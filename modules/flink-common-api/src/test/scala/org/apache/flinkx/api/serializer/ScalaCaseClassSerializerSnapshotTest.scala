package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.base.{IntSerializer, StringSerializer}
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.serializer.ScalaCaseClassSerializerSnapshotTest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ScalaCaseClassSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  "CaseClassSerializer.snapshotConfiguration" should "produce a ScalaCaseClassSerializerSnapshot" in {
    serializerOf(stringSer).snapshotConfiguration() should be(a[ScalaCaseClassSerializerSnapshot[_]])
  }

  "resolveSchemaCompatibility" should "be compatibleAsIs when the schema is unchanged" in {
    val registered = serializerOf(stringSer).snapshotConfiguration()
    val restored   = savepointed[Evolved](serializerOf(stringSer))

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("compatibleAsIs")
  }

  it should "be compatibleAfterMigration when a field is appended to the end" in {
    val registered = serializerOf(stringSer, intSer).snapshotConfiguration() // new code: 2 fields
    val restored   = savepointed[Evolved](serializerOf(stringSer))           // savepoint: 1 field

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("compatibleAfterMigration")
  }

  it should "be incompatible when a field is removed" in {
    val registered = serializerOf(stringSer).snapshotConfiguration()       // new code: 1 field
    val restored   = savepointed[Evolved](serializerOf(stringSer, intSer)) // savepoint: 2 fields

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("incompatible")
  }

  it should "be incompatible when a shared leading field changes type" in {
    val registered = serializerOf(intSer).snapshotConfiguration()
    val restored   = savepointed[Evolved](serializerOf(stringSer))

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("incompatible")
  }

  it should "be incompatible when the class name differs" in {
    val registered = serializerOf(stringSer).snapshotConfiguration()
    // A snapshot of a different class is erased to the same raw type at runtime, the way Flink sees it.
    val restored = savepointed[Other](new CaseClassSerializer[Other](classOf[Other], Array(stringSer), false))
      .asInstanceOf[TypeSerializerSnapshot[Evolved]]

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("incompatible")
  }

  // Guards that we still emit the exact byte layout CompositeTypeSerializerSnapshot used to write, so savepoints taken
  // before this snapshot stopped extending it keep restoring unchanged.
  "the snapshot wire format" should "start with the composite magic number, outer version and class name" in {
    val out = new DataOutputSerializer(1024)
    serializerOf(stringSer).snapshotConfiguration().writeSnapshot(out)

    val in = new DataInputDeserializer(out.getSharedBuffer)
    in.readInt() should be(911108)                   // CompositeTypeSerializerSnapshot.MAGIC_NUMBER
    in.readInt() should be(3)                        // outer-snapshot version
    in.readUTF() should be(classOf[Evolved].getName) // outer snapshot: class name
  }

  it should "round-trip through writeSnapshot/readSnapshot" in {
    val original = serializerOf(stringSer, intSer).snapshotConfiguration()

    val out = new DataOutputSerializer(1024)
    original.writeSnapshot(out)

    val restored = new ScalaCaseClassSerializerSnapshot[Evolved]()
    restored.readSnapshot(
      original.getCurrentVersion,
      new DataInputDeserializer(out.getSharedBuffer),
      getClass.getClassLoader
    )

    restored.restoreSerializer().asInstanceOf[CaseClassSerializer[Evolved]].getArity should be(2)
    original.resolveSchemaCompatibility(restored) shouldBe Symbol("compatibleAsIs")
  }

  "an appended field" should "be read from old data with the restored serializer and filled with its default" in {
    val oldSer = serializerOf(stringSer) // old code: single field

    val out = new DataOutputSerializer(1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, oldSer.snapshotConfiguration())
    oldSer.serialize(Evolved("x"), out)

    val in              = new DataInputDeserializer(out.getSharedBuffer)
    val restoredOldSnap = TypeSerializerSnapshot.readVersionedSnapshot[Evolved](in, getClass.getClassLoader)

    val newSer = serializerOf(stringSer, intSer) // new code: appended `b`
    newSer.snapshotConfiguration().resolveSchemaCompatibility(restoredOldSnap) shouldBe Symbol(
      "compatibleAfterMigration"
    )

    // Flink reads the old bytes with the serializer restored from the savepoint; the missing `b` gets its default.
    restoredOldSnap.restoreSerializer().deserialize(in) should be(Evolved("x", 0))
  }

}

object ScalaCaseClassSerializerSnapshotTest {

  case class Evolved(a: String, b: Int = 0)
  case class Other(a: String)

  val stringSer: TypeSerializer[_] = StringSerializer.INSTANCE
  val intSer: TypeSerializer[_]    = IntSerializer.INSTANCE

  def serializerOf(fieldSerializers: TypeSerializer[_]*): CaseClassSerializer[Evolved] =
    new CaseClassSerializer[Evolved](classOf[Evolved], fieldSerializers.toArray, false)

  /** Writes the serializer's snapshot then reads it back, the way a savepoint does. */
  def savepointed[T](serializer: TypeSerializer[_]): TypeSerializerSnapshot[T] =
    savepointedSnapshot[T](serializer.snapshotConfiguration())

  /** Writes a snapshot then reads it back, the way a savepoint does. */
  def savepointedSnapshot[T](snapshot: TypeSerializerSnapshot[_]): TypeSerializerSnapshot[T] = {
    val out = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, snapshot)
    val in = new DataInputDeserializer(out.getSharedBuffer)
    TypeSerializerSnapshot.readVersionedSnapshot[T](in, getClass.getClassLoader)
  }

}
