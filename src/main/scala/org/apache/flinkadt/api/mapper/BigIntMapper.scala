package org.apache.flinkadt.api.mapper

import org.apache.flinkadt.api.serializer.MappedSerializer.TypeMapper

import java.math.BigInteger

class BigIntMapper() extends TypeMapper[scala.BigInt, java.math.BigInteger] {
  override def contramap(b: BigInteger): BigInt = BigInt(b)
  override def map(a: BigInt): BigInteger       = a.bigInteger
}
