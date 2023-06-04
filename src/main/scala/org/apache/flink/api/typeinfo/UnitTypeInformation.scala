package org.apache.flink.api.typeinfo

import org.apache.flink.api.serializer.UnitSerializer
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

class UnitTypeInformation extends TypeInformation[Unit] {
  override def createSerializer(config: ExecutionConfig): TypeSerializer[Unit] = new UnitSerializer()
  override def isKeyType: Boolean                                              = true
  override def getTotalFields: Int                                             = 0
  override def isTupleType: Boolean                                            = false
  override def canEqual(obj: Any): Boolean                                     = obj.isInstanceOf[Unit]
  override def getTypeClass: Class[Unit]                                       = classOf[Unit]
  override def getArity: Int                                                   = 0
  override def isBasicType: Boolean                                            = false

  override def toString: String = "{}"

  override def equals(obj: Any): Boolean = obj match {
    case _: Unit => true
    case _       => false
  }

  override def hashCode(): Int = ().hashCode()
}
