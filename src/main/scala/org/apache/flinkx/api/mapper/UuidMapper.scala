package org.apache.flinkx.api.mapper

import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper

import java.nio.ByteBuffer
import java.util.UUID

class UuidMapper() extends TypeMapper[UUID, Array[Byte]] {
  override def map(a: UUID): Array[Byte] = {
    val buffer = ByteBuffer.allocate(16)
    buffer.putLong(a.getMostSignificantBits)
    buffer.putLong(a.getLeastSignificantBits)
    buffer.array()
  }

  override def contramap(b: Array[Byte]): UUID = {
    val buffer               = ByteBuffer.wrap(b)
    val mostSignificantBits  = buffer.getLong()
    val leastSignificantBits = buffer.getLong()
    new UUID(mostSignificantBits, leastSignificantBits)
  }
}
