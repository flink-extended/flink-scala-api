package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil

/** Generic serializer snapshot for collection.
  *
  * @param nestedSerializer
  *   the serializer of `T`
  * @param clazz
  *   the class of `S`
  * @param vclazz
  *   the class of `T`
  * @tparam F
  *   the type of the serialized collection
  * @tparam T
  *   the type of the collection's elements
  * @tparam S
  *   the type of the collection serializer
  */
class CollectionSerializerSnapshot[F[_], T, S <: TypeSerializer[F[T]]](
    var nestedSerializer: TypeSerializer[T],
    var clazz: Class[S],
    var vclazz: Class[T]
) extends TypeSerializerSnapshot[F[T]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(null, null, null)

  private var currentVersionCalled = false
  private var writeSnapshotCalled  = false

  override def getCurrentVersion: Int = {
    currentVersionCalled = true
    2
  }

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    clazz = InstantiationUtil.resolveClassByName[S](in, userCodeClassLoader)
    vclazz = InstantiationUtil.resolveClassByName[T](in, userCodeClassLoader)
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
      nestedSerializer = TypeSerializerSnapshot.readVersionedSnapshot[T](in, userCodeClassLoader).restoreSerializer()
    } else {
      val snapClass      = InstantiationUtil.resolveClassByName[TypeSerializerSnapshot[T]](in, userCodeClassLoader)
      val nestedSnapshot = InstantiationUtil.instantiate(snapClass)
      nestedSnapshot.readSnapshot(nestedSnapshot.getCurrentVersion, in, userCodeClassLoader)
      nestedSerializer = nestedSnapshot.restoreSerializer()
    }
  }

  override def writeSnapshot(out: DataOutputView): Unit = {
    writeSnapshotCalled = true
    out.writeUTF(clazz.getName)
    vclazz.getName match {
      case "double"  => out.writeUTF("java.lang.Double")
      case "float"   => out.writeUTF("java.lang.Float")
      case "int"     => out.writeUTF("java.lang.Integer")
      case "long"    => out.writeUTF("java.lang.Long")
      case "byte"    => out.writeUTF("java.lang.Byte")
      case "short"   => out.writeUTF("java.lang.Short")
      case "char"    => out.writeUTF("java.lang.Character")
      case "boolean" => out.writeUTF("java.lang.Boolean")
      case "void"    => out.writeUTF("java.lang.Void")
      case other     => out.writeUTF(other)
    }
    TypeSerializerSnapshot.writeVersionedSnapshot(out, nestedSerializer.snapshotConfiguration())
  }

  override def resolveSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[F[T]]
  ): TypeSerializerSchemaCompatibility[F[T]] = TypeSerializerSchemaCompatibility.compatibleAsIs()

  override def restoreSerializer(): TypeSerializer[F[T]] = {
    val constructor = clazz.getConstructors()(0)
    constructor.newInstance(nestedSerializer, vclazz).asInstanceOf[TypeSerializer[F[T]]]
  }

}
