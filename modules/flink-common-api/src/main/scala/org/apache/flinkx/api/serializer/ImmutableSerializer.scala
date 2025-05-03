package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

abstract class ImmutableSerializer[T] extends TypeSerializerSingleton[T] {
  override def isImmutableType: Boolean                                  = true
  override def copy(from: T): T                                          = from
  override def copy(from: T, reuse: T): T                                = from
  override def deserialize(reuse: T, source: DataInputView): T           = deserialize(source)
  override def copy(source: DataInputView, target: DataOutputView): Unit = serialize(deserialize(source), target)
}
