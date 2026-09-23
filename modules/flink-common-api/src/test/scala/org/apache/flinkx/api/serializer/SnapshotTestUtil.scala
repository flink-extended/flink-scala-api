package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataInputView, DataOutputSerializer, DataOutputView}
import org.apache.flinkx.api.auto._

/** Data and serializers shared by the tests resolving the schema compatibility of serializer snapshots. */
object SnapshotTestUtil {

  case class Foo(a: String)
  case class Bar(b: Int)

  val fooSerializer: TypeSerializer[Foo] = implicitly[TypeSerializer[Foo]]
  val barSerializer: TypeSerializer[Bar] = implicitly[TypeSerializer[Bar]]

  /** Writes the snapshot of the serializer then reads it back, the way a savepoint does. */
  def savepointed[T](serializer: TypeSerializer[_]): TypeSerializerSnapshot[T] = {
    val out = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration())
    val in = new DataInputDeserializer(out.getSharedBuffer)
    TypeSerializerSnapshot.readVersionedSnapshot[T](in, getClass.getClassLoader)
  }

  /** Serializer of [[String]] asking for a reconfiguration when the version it is restored from differs. */
  class VersionedSerializer(val version: Int) extends ImmutableSerializer[String] {
    override def createInstance(): String                                = ""
    override def getLength: Int                                          = -1
    override def serialize(record: String, target: DataOutputView): Unit = target.writeUTF(record)
    override def deserialize(source: DataInputView): String              = source.readUTF()
    override def snapshotConfiguration(): TypeSerializerSnapshot[String] = new VersionedSerializerSnapshot(version)
    override def equals(other: Any): Boolean                             = other match {
      case that: VersionedSerializer => that.version == version
      case _                         => false
    }
    override def hashCode(): Int = version
  }

  class VersionedSerializerSnapshot(private var version: Int) extends TypeSerializerSnapshot[String] {

    def this() = this(0)

    override def getCurrentVersion: Int = 1

    override def writeSnapshot(out: DataOutputView): Unit = out.writeInt(version)

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit =
      version = in.readInt()

    override def restoreSerializer(): TypeSerializer[String] = new VersionedSerializer(version)

    override def resolveSchemaCompatibility(
        restoredSnapshot: TypeSerializerSnapshot[String]
    ): TypeSerializerSchemaCompatibility[String] = restoredSnapshot match {
      case restored: VersionedSerializerSnapshot if restored.version == version =>
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      case _: VersionedSerializerSnapshot =>
        TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(new VersionedSerializer(version))
      case _ =>
        TypeSerializerSchemaCompatibility.incompatible()
    }

  }

}
