package org.apache.flinkx.api.typeinfo

import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

import scala.reflect.{classTag, ClassTag}

abstract class SimpleTypeInformation[T: ClassTag: TypeSerializer] extends TypeInformation[T] {
  override def createSerializer(config: SerializerConfig): TypeSerializer[T] = {
    val ser = implicitly[TypeSerializer[T]]
    if (ser.isImmutableType) ser
    else ser.duplicate()
  }
  override def isBasicType: Boolean   = false
  override def isTupleType: Boolean   = false
  override def isKeyType: Boolean     = false
  override def getTotalFields: Int    = 1
  override def getTypeClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  override def getArity: Int          = 1
}
