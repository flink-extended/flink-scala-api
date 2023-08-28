package org.apache.flinkx.api.serializer

import org.apache.flinkx.api.serializer.UnitSerializer.UnitSerializerSnapshot
import org.apache.flink.api.common.typeutils.{SimpleTypeSerializerSnapshot, TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import java.util.function.Supplier

class UnitSerializer extends SimpleSerializer[Unit] {
  override def getLength: Int = 0

  override def serialize(record: Unit, target: DataOutputView): Unit = {}

  override def deserialize(reuse: Unit, source: DataInputView): Unit = {}

  override def deserialize(source: DataInputView): Unit = {}

  override def snapshotConfiguration(): TypeSerializerSnapshot[Unit] = new UnitSerializerSnapshot()

  override def createInstance(): Unit = {}
}

object UnitSerializer {
  class UnitSerializerSnapshot
      extends SimpleTypeSerializerSnapshot[Unit](
        new Supplier[TypeSerializer[Unit]] {
          override def get(): TypeSerializer[Unit] = new UnitSerializer
        }
      )
}
