package org.apache.flinkx.api.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.serializer.UnitSerializer

class UnitTypeInformation extends TypeInformation[Unit] {

  override def createSerializer(config: SerializerConfig): TypeSerializer[Unit] = new UnitSerializer()
  // override modifier removed to satisfy both implementation requirement of Flink 1.x and removal in 2.x
  def createSerializer(config: ExecutionConfig): TypeSerializer[Unit] = new UnitSerializer()

  override def isKeyType: Boolean          = false
  override def getTotalFields: Int         = 1 // The total number of fields must be at least 1.
  override def isTupleType: Boolean        = false
  override def canEqual(obj: Any): Boolean = obj.isInstanceOf[UnitTypeInformation]
  override def getTypeClass: Class[Unit]   = classOf[Unit]
  override def getArity: Int               = 1 // Basic type has an arity of 1
  override def isBasicType: Boolean        = true

  override def toString: String = "{}"

  override def equals(obj: Any): Boolean = obj match {
    case _: UnitTypeInformation => true
    case _                      => false
  }

  override def hashCode(): Int = ().hashCode()
}
