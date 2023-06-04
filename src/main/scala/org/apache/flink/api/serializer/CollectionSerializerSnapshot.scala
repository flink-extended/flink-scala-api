package org.apache.flink.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil

class CollectionSerializerSnapshot[F[_], T, S <: TypeSerializer[F[T]]]() extends TypeSerializerSnapshot[F[T]] {
  def this(ser: TypeSerializer[T], serClass: Class[S], valueClass: Class[T]) = {
    this()
    nestedSerializer = ser
    clazz = serClass
    vclazz = valueClass
  }

  var nestedSerializer: TypeSerializer[T] = _
  var clazz: Class[S]                     = _
  var vclazz: Class[T]                    = _

  override def getCurrentVersion: Int = 1

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    clazz = InstantiationUtil.resolveClassByName[S](in, userCodeClassLoader)
    vclazz = InstantiationUtil.resolveClassByName[T](in, userCodeClassLoader)
    val snapClass      = InstantiationUtil.resolveClassByName[TypeSerializerSnapshot[T]](in, userCodeClassLoader)
    val nestedSnapshot = InstantiationUtil.instantiate(snapClass)
    nestedSnapshot.readSnapshot(nestedSnapshot.getCurrentVersion, in, userCodeClassLoader)
    nestedSerializer = nestedSnapshot.restoreSerializer()
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
      case "char"    => out.writeUTF("java.lang.Char")
      case "boolean" => out.writeUTF("java.lang.Boolean")
      case other     => out.writeUTF(other)
    }

    out.writeUTF(nestedSerializer.snapshotConfiguration().getClass.getName)
    nestedSerializer.snapshotConfiguration().writeSnapshot(out)
  }

  override def resolveSchemaCompatibility(
      newSerializer: TypeSerializer[F[T]]
  ): TypeSerializerSchemaCompatibility[F[T]] = TypeSerializerSchemaCompatibility.compatibleAsIs()

  override def restoreSerializer(): TypeSerializer[F[T]] = {
    val constructor = clazz.getConstructors()(0)
    constructor.newInstance(nestedSerializer, vclazz).asInstanceOf[TypeSerializer[F[T]]]
  }
}
