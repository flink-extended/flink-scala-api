package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.serializer.CoproductSerializer.CoproductSerializerSnapshot

class CoproductSerializer[T](subtypeClasses: Array[Class[_]], subtypeSerializers: Array[TypeSerializer[_]])
    extends TypeSerializerSingleton[T] {
  override def isImmutableType: Boolean                                  = true
  override def copy(from: T): T                                          = from
  override def copy(from: T, reuse: T): T                                = from
  override def copy(source: DataInputView, target: DataOutputView): Unit = serialize(deserialize(source), target)
  override def createInstance(): T =
    // this one may be used for later reuse, but we never reuse coproducts due to their unclear concrete type
    subtypeSerializers.head.createInstance().asInstanceOf[T]
  override def getLength: Int = -1
  override def serialize(record: T, target: DataOutputView): Unit = {
    var subtypeIndex = 0
    var found        = false
    while (!found && (subtypeIndex < subtypeClasses.length)) {
      if (subtypeClasses(subtypeIndex).isInstance(record)) {
        found = true
      } else {
        subtypeIndex += 1
      }
    }
    if (found) {
      target.writeByte(subtypeIndex.toByte.toInt)
      subtypeSerializers(subtypeIndex).asInstanceOf[TypeSerializer[T]].serialize(record, target)
    } else {
      throw new IllegalStateException("subtype not found in sealed trait schema")
    }
  }

  override def deserialize(source: DataInputView): T = {
    val index   = source.readByte()
    val subtype = subtypeSerializers(index.toInt)
    subtype.asInstanceOf[TypeSerializer[T]].deserialize(source)
  }
  override def deserialize(reuse: T, source: DataInputView): T = deserialize(source)
  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new CoproductSerializerSnapshot(subtypeClasses, subtypeSerializers)
}

object CoproductSerializer {
  class CoproductSerializerSnapshot[T](
      var subtypeClasses: Array[Class[_]],
      var subtypeSerializers: Array[TypeSerializer[_]]
  ) extends TypeSerializerSnapshot[T] {

    // Empty constructor is required to instantiate this class during deserialization.
    def this() = this(Array.empty[Class[_]], Array.empty[TypeSerializer[_]])

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
      val len = in.readInt()

      subtypeClasses = (0 until len)
        .map(_ => InstantiationUtil.resolveClassByName(in, userCodeClassLoader))
        .toArray

      subtypeSerializers = (0 until len)
        .map(_ => TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader).restoreSerializer())
        .toArray
    }

    override def getCurrentVersion: Int = 2

    override def writeSnapshot(out: DataOutputView): Unit = {
      out.writeInt(subtypeClasses.length)
      subtypeClasses.foreach(c => out.writeUTF(c.getName))
      subtypeSerializers.foreach(s => {
        TypeSerializerSnapshot.writeVersionedSnapshot(out, s.snapshotConfiguration())
      })
    }

    override def resolveSchemaCompatibility(
        oldSerializerSnapshot: TypeSerializerSnapshot[T]
    ): TypeSerializerSchemaCompatibility[T] =
      TypeSerializerSchemaCompatibility.compatibleAsIs()

    override def restoreSerializer(): TypeSerializer[T] =
      new CoproductSerializer[T](subtypeClasses, subtypeSerializers)
  }

}
