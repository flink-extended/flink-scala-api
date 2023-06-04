package org.apache.flink.api.mapper

import org.apache.flink.api.serializer.MappedSerializer.TypeMapper

import java.math.BigInteger

class BigIntMapper() extends TypeMapper[scala.BigInt, java.math.BigInteger] {
  override def contramap(b: BigInteger): BigInt = BigInt(b)
  override def map(a: BigInt): BigInteger       = a.bigInteger
}
