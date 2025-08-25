package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class VectorSerializer[T](child: TypeSerializer[T], clazz: Class[T]) extends MutableSerializer[Vector[T]] {

  override val isImmutableType: Boolean = child.isImmutableType

  override def copy(from: Vector[T]): Vector[T] = {
    if (from == null || isImmutableType) {
      from
    } else {
      from.map(child.copy)
    }
  }

  override def duplicate(): VectorSerializer[T] = {
    val duplicatedChild = child.duplicate()
    if (duplicatedChild.eq(child)) {
      this
    } else {
      new VectorSerializer[T](duplicatedChild, clazz)
    }
  }

  override def createInstance(): Vector[T]                   = Vector.empty[T]
  override def getLength: Int                                = -1
  override def deserialize(source: DataInputView): Vector[T] = {
    var remaining = source.readInt()
    val builder   = Vector.newBuilder[T]
    builder.sizeHint(remaining)
    while (remaining > 0) {
      builder.addOne(child.deserialize(source))
      remaining -= 1
    }
    builder.result()
  }
  override def serialize(record: Vector[T], target: DataOutputView): Unit = {
    target.writeInt(record.size)
    record.foreach(element => child.serialize(element, target))
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    var remaining = source.readInt()
    target.writeInt(remaining)
    while (remaining > 0) {
      child.copy(source, target)
      remaining -= 1
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[Vector[T]] =
    new CollectionSerializerSnapshot[Vector, T, VectorSerializer[T]](child, classOf[VectorSerializer[T]], clazz)

}
