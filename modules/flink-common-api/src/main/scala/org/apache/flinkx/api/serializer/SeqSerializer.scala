package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class SeqSerializer[T](child: TypeSerializer[T], clazz: Class[T]) extends MutableSerializer[Seq[T]] {

  override val isImmutableType: Boolean = child.isImmutableType

  override def copy(from: Seq[T]): Seq[T] = {
    if (from == null || isImmutableType) {
      from
    } else {
      from.map(child.copy)
    }
  }

  override def duplicate(): SeqSerializer[T] = {
    val duplicatedChild = child.duplicate()
    if (duplicatedChild.eq(child)) {
      this
    } else {
      new SeqSerializer[T](duplicatedChild, clazz)
    }
  }

  override def createInstance(): Seq[T]                   = Seq.empty[T]
  override def getLength: Int                             = -1
  override def deserialize(source: DataInputView): Seq[T] = {
    val count  = source.readInt()
    val result = for {
      _ <- 0 until count
    } yield {
      child.deserialize(source)
    }
    result
  }
  override def serialize(record: Seq[T], target: DataOutputView): Unit = {
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

  override def snapshotConfiguration(): TypeSerializerSnapshot[Seq[T]] =
    new CollectionSerializerSnapshot[Seq, T, SeqSerializer[T]](child, classOf[SeqSerializer[T]], clazz)

}
