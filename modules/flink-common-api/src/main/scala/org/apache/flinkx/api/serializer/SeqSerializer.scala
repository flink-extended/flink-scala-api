package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.VariableLengthDataType

import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag

class SeqSerializer[T](child: TypeSerializer[T], clazz: Class[T]) extends MutableSerializer[Seq[T]] {

  private implicit val classTag: ClassTag[T] = ClassTag(clazz)

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
  override def getLength: Int                             = VariableLengthDataType
  override def deserialize(source: DataInputView): Seq[T] = {
    val length = source.readInt()
    val array  = new Array[T](length)
    var i      = 0
    while (i < length) {
      array(i) = child.deserialize(source)
      i += 1
    }
    ArraySeq.unsafeWrapArray(array)
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
