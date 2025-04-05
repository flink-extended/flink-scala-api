package org.apache.flinkx.api.typeinfo

import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

class ProductTypeInformation[T <: Product](
    c: Class[T],
    fieldTypes: Seq[TypeInformation[_]],
    fieldNames: Seq[String],
    ser: TypeSerializer[T]
) extends CaseClassTypeInfo[T](
      clazz = c,
      typeParamTypeInfos = Array(),
      fieldTypes,
      fieldNames
    ) {
  override def createSerializer(config: SerializerConfig): TypeSerializer[T] =
    if (ser.isImmutableType) ser
    else ser.duplicate()
}
