package org.apache.flinkx.api.mapper

import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper

class BigDecMapper extends TypeMapper[scala.BigDecimal, java.math.BigDecimal] {
  override def map(a: BigDecimal): java.math.BigDecimal       = a.bigDecimal
  override def contramap(b: java.math.BigDecimal): BigDecimal = BigDecimal(b)
}
