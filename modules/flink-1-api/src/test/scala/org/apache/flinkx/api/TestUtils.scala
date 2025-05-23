package org.apache.flinkx.api

import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.scalatest.{Assertion, Inspectors}
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}

trait TestUtils extends Matchers with Inspectors {
  def roundtrip[T](ser: TypeSerializer[T], in: T): Assertion = {
    val out = new ByteArrayOutputStream()
    ser.serialize(in, new DataOutputViewStreamWrapper(out))
    val snapBytes = new ByteArrayOutputStream()
    TypeSerializerSnapshot.writeVersionedSnapshot(
      new DataOutputViewStreamWrapper(snapBytes),
      ser.snapshotConfiguration()
    )
    val restoredSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[T](
      new DataInputViewStreamWrapper(new ByteArrayInputStream(snapBytes.toByteArray)),
      ser.getClass.getClassLoader
    )
    val restoredSerializer = restoredSnapshot.restoreSerializer()
    val copy = restoredSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(out.toByteArray)))
    in shouldBe copy
  }

  def noKryo[T](ser: TypeSerializer[T]): Unit =
    ser match {
      case p: CaseClassSerializer[_] =>
        forAll(p.getFieldSerializers) { param =>
          noKryo(param)
        }
      case _: KryoSerializer[_] =>
        throw new IllegalArgumentException("kryo detected")
      case _ => // ok
    }

  def serializable[T](ser: TypeSerializer[T]): Unit = {
    val stream = new ObjectOutputStream(new ByteArrayOutputStream())
    stream.writeObject(ser)
  }

  def all[T](ser: TypeSerializer[T], in: T): Unit = {
    roundtrip(ser, in)
    noKryo(ser)
    serializable(ser)
  }

}
