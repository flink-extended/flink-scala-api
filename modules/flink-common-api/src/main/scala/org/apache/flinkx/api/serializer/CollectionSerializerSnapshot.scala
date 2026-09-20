package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.serializer.CollectionSerializerSnapshot.CurrentVersion

/** Generic serializer snapshot for collection.
  *
  * @param nestedSnapshot
  *   the snapshot of the serializer of `T`
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
    var nestedSnapshot: TypeSerializerSnapshot[T],
    var clazz: Class[S],
    var vclazz: Class[T]
) extends TypeSerializerSnapshot[F[T]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(null, null, null)

  override def getCurrentVersion: Int = CurrentVersion

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    clazz = InstantiationUtil.resolveClassByName[S](in, userCodeClassLoader)
    vclazz = InstantiationUtil.resolveClassByName[T](in, userCodeClassLoader)
    nestedSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[T](in, userCodeClassLoader)
  }

  override def writeSnapshot(out: DataOutputView): Unit = {
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
    TypeSerializerSnapshot.writeVersionedSnapshot(out, nestedSnapshot)
  }

  /** Compatible when the same collection is serialized and the serializers of its elements are compatible. */
  override def resolveSchemaCompatibility(
      restoredSnapshot: TypeSerializerSnapshot[F[T]]
  ): TypeSerializerSchemaCompatibility[F[T]] = restoredSnapshot match {
    case restored: CollectionSerializerSnapshot[_, _, _]
        if restored.getClass == getClass && restored.clazz.getName == clazz.getName =>
      SerializerUtil.resolveNestedSchemaCompatibility(nestedSnapshots, restored.nestedSnapshots, restoreSerializerWith)
    case _ =>
      TypeSerializerSchemaCompatibility.incompatible()
  }

  /** The snapshots of the nested serializers, in the order [[restoreSerializerWith]] expects their serializers. */
  protected def nestedSnapshots: Array[TypeSerializerSnapshot[_]] = Array(nestedSnapshot)

  /** Restores the collection serializer with the given nested serializers. */
  protected def restoreSerializerWith(nestedSerializers: Array[TypeSerializer[_]]): TypeSerializer[F[T]] = {
    val constructor = clazz.getConstructors()(0)
    constructor.newInstance(nestedSerializers(0), vclazz).asInstanceOf[TypeSerializer[F[T]]]
  }

  override def restoreSerializer(): TypeSerializer[F[T]] =
    restoreSerializerWith(Array(nestedSnapshot.restoreSerializer()))

}

object CollectionSerializerSnapshot {
  private val CurrentVersion = 2
}
