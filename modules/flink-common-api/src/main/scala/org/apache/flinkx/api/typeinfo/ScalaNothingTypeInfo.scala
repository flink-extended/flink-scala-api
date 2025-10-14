package org.apache.flinkx.api.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.serializer.NothingSerializer

object ScalaNothingTypeInfo extends TypeInformation[Nothing] {

  override def createSerializer(config: SerializerConfig): TypeSerializer[Nothing] =
    new NothingSerializer().asInstanceOf[TypeSerializer[Nothing]]
  // override modifier removed to satisfy both implementation requirement of Flink 1.x and removal in 2.x
  def createSerializer(config: ExecutionConfig): TypeSerializer[Nothing] =
    new NothingSerializer().asInstanceOf[TypeSerializer[Nothing]]

  override def isKeyType: Boolean   = false
  override def getTotalFields: Int  = 11 // At runtime scala.Nothing is scala.runtime.Nothing$ and extends Throwable
  override def isTupleType: Boolean = false
  override def canEqual(obj: Any): Boolean  = obj.isInstanceOf[ScalaNothingTypeInfo.type]
  override def getTypeClass: Class[Nothing] = classOf[Nothing]
  override def getArity: Int             = 6 // At runtime scala.Nothing is scala.runtime.Nothing$ and extends Throwable
  override def isBasicType: Boolean      = false
  override def toString: String          = "ScalaNothingTypeInfo()"
  override def equals(obj: Any): Boolean = this.eq(obj.asInstanceOf[AnyRef])
  override def hashCode(): Int           = 0

}
