package org.apache.flinkx.api.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

case object MarkerTypeInfo extends TypeInformation[Nothing] {

  override def createSerializer(config: SerializerConfig): TypeSerializer[Nothing] = null
  // override modifier removed to satisfy both implementation requirement of Flink 1.x and removal in 2.x
  def createSerializer(config: ExecutionConfig): TypeSerializer[Nothing] = null

  override def isBasicType: Boolean         = false
  override def isTupleType: Boolean         = false
  override def isKeyType: Boolean           = false
  override def getTotalFields: Int          = 1
  override def getTypeClass: Class[Nothing] = classOf[Nothing]
  override def getArity: Int                = 1
  override def toString: String             = "MarkerTypeInfo"
  override def equals(obj: Any): Boolean    = obj.isInstanceOf[MarkerTypeInfo.type]
  override def hashCode(): Int              = 0

}
