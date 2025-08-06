package org.apache.flinkx.api.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.serializer.MappedSerializer
import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper

import scala.reflect.{ClassTag, classTag}

case class MappedTypeInformation[A: ClassTag, B](mapper: TypeMapper[A, B], nested: TypeInformation[B])
    extends TypeInformation[A] {

  override def createSerializer(config: SerializerConfig): TypeSerializer[A] =
    new MappedSerializer(mapper, nested.createSerializer(config))
  // override modifier removed to satisfy both implementation requirement of Flink 1.x and removal in 2.x
  def createSerializer(config: ExecutionConfig): TypeSerializer[A] = createSerializer(config.getSerializerConfig)

  override def isKeyType: Boolean   = nested.isKeyType
  override def getTotalFields: Int  = nested.getTotalFields
  override def isTupleType: Boolean = nested.isTupleType

  override def canEqual(obj: Any): Boolean = obj match {
    case _: MappedTypeInformation[_, _] => true
    case _                              => false
  }
  override def getTypeClass: Class[A] = classTag[A].runtimeClass.asInstanceOf[Class[A]]
  override def getArity: Int          = nested.getArity
  override def isBasicType: Boolean   = nested.isBasicType

  override def toString: String = nested.toString

  override def equals(obj: Any): Boolean = obj match {
    case m: MappedTypeInformation[_, _] => (m.nested == nested) && m.mapper.equals(mapper)
    case _                              => false
  }

  override def hashCode(): Int = nested.hashCode()

}
