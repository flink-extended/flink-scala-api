package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.serializer.MapSerializer._

class MapSerializer[K, V](ks: TypeSerializer[K], vs: TypeSerializer[V]) extends MutableSerializer[Map[K, V]] {

  override val isImmutableType: Boolean = ks.isImmutableType && vs.isImmutableType

  override def copy(from: Map[K, V]): Map[K, V] = {
    if (from == null || isImmutableType) {
      from
    } else {
      from.map(element => (ks.copy(element._1), vs.copy(element._2)))
    }
  }

  override def duplicate(): MapSerializer[K, V] = {
    val duplicatedKS = ks.duplicate()
    val duplicatedVS = vs.duplicate()
    if (duplicatedKS.eq(ks) && duplicatedVS.eq(vs)) {
      this
    } else {
      new MapSerializer[K, V](duplicatedKS, duplicatedVS)
    }
  }

  override def createInstance(): Map[K, V]                   = Map.empty[K, V]
  override def getLength: Int                                = -1
  override def deserialize(source: DataInputView): Map[K, V] = {
    val count  = source.readInt()
    val result = for {
      _ <- 0 until count
    } yield {
      val key   = ks.deserialize(source)
      val value = vs.deserialize(source)
      key -> value
    }
    result.toMap
  }
  override def serialize(record: Map[K, V], target: DataOutputView): Unit = {
    target.writeInt(record.size)
    record.foreach(element => {
      ks.serialize(element._1, target)
      vs.serialize(element._2, target)
    })
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[Map[K, V]] = new MapSerializerSnapshot(ks, vs)
}

object MapSerializer {
  case class MapSerializerSnapshot[K, V](var keySerializer: TypeSerializer[K], var valueSerializer: TypeSerializer[V])
      extends TypeSerializerSnapshot[Map[K, V]] {

    def this() = this(null, null)

    private var currentVersionCalled = false
    private var writeSnapshotCalled  = false

    override def getCurrentVersion: Int = {
      currentVersionCalled = true
      2
    }

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
      if (
        /* - The old code was calling getCurrentVersion() just before calling readSnapshot().
           If only getCurrentVersion() is called, we know we must deserialize with old behavior.
         - The new code calls getCurrentVersion() only before calling writeSnapshot().
           getCurrentVersion() is not called before calling readSnapshot()
           or both getCurrentVersion() and writeSnapshot() are called,
           so in these cases we know the readVersion parameter is trustable to determine which behavior to apply. */
        (!currentVersionCalled || writeSnapshotCalled) &&
        // readVersion is trustable
        readVersion == 2
      ) {
        keySerializer = TypeSerializerSnapshot.readVersionedSnapshot[K](in, userCodeClassLoader).restoreSerializer()
        valueSerializer = TypeSerializerSnapshot.readVersionedSnapshot[V](in, userCodeClassLoader).restoreSerializer()
      } else {
        keySerializer = readSerializer[K](in, userCodeClassLoader)
        valueSerializer = readSerializer[V](in, userCodeClassLoader)
      }
    }

    def readSerializer[T](in: DataInputView, userCodeClassLoader: ClassLoader): TypeSerializer[T] = {
      val snapClass      = InstantiationUtil.resolveClassByName[TypeSerializerSnapshot[T]](in, userCodeClassLoader)
      val nestedSnapshot = InstantiationUtil.instantiate(snapClass)
      nestedSnapshot.readSnapshot(nestedSnapshot.getCurrentVersion, in, userCodeClassLoader)
      nestedSnapshot.restoreSerializer()
    }

    override def writeSnapshot(out: DataOutputView): Unit = {
      writeSnapshotCalled = true
      TypeSerializerSnapshot.writeVersionedSnapshot(out, keySerializer.snapshotConfiguration())
      TypeSerializerSnapshot.writeVersionedSnapshot(out, valueSerializer.snapshotConfiguration())
    }

    override def resolveSchemaCompatibility(
        newSerializer: TypeSerializer[Map[K, V]]
    ): TypeSerializerSchemaCompatibility[Map[K, V]] = TypeSerializerSchemaCompatibility.compatibleAsIs()

    override def restoreSerializer(): TypeSerializer[Map[K, V]] = new MapSerializer(keySerializer, valueSerializer)
  }

}
