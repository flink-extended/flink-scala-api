package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/** Generic serializer snapshot for sorted collection.
  * @param aSnapshot
  *   the snapshot of the serializer of `A`
  * @param aOrderingSnapshot
  *   the snapshot of the serializer of `Ordering[A]`
  * @param sClass
  *   the class of `S`
  * @param aClass
  *   the class of `A`
  * @tparam F
  *   the type of the serialized collection
  * @tparam A
  *   the type of the collection's elements
  * @tparam S
  *   the type of the collection serializer
  */
class SortedCollectionSerializerSnapshot[F[_], A, S <: TypeSerializer[F[A]]](
    aSnapshot: TypeSerializerSnapshot[A],
    sClass: Class[S],
    aClass: Class[A],
    private var aOrderingSnapshot: TypeSerializerSnapshot[Ordering[A]]
) extends CollectionSerializerSnapshot[F, A, S](aSnapshot, sClass, aClass) {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(null, null, null, null)

  override def getCurrentVersion: Int = super.getCurrentVersion // Must be aligned with CollectionSerializerSnapshot

  override def writeSnapshot(out: DataOutputView): Unit = {
    super.writeSnapshot(out)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, aOrderingSnapshot)
  }

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    super.readSnapshot(readVersion, in, userCodeClassLoader)
    aOrderingSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[Ordering[A]](in, userCodeClassLoader)
  }

  override protected def nestedSnapshots: Array[TypeSerializerSnapshot[_]] = super.nestedSnapshots :+ aOrderingSnapshot

  override protected def restoreSerializerWith(nestedSerializers: Array[TypeSerializer[_]]): TypeSerializer[F[A]] = {
    val constructor = clazz.getConstructors()(0)
    constructor.newInstance(nestedSerializers(0), vclazz, nestedSerializers(1)).asInstanceOf[TypeSerializer[F[A]]]
  }

  override def restoreSerializer(): TypeSerializer[F[A]] =
    restoreSerializerWith(Array(nestedSnapshot.restoreSerializer(), aOrderingSnapshot.restoreSerializer()))

}
