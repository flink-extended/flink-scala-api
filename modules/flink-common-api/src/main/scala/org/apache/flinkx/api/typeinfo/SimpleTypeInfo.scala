package org.apache.flinkx.api.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

import scala.reflect.{ClassTag, classTag}

final case class SimpleTypeInfo[T: ClassTag: TypeSerializer](
    arity: Int = 1,
    totalFields: Int = 1, // The total number of fields must be at least 1.
    basicType: Boolean = false,
    tupleType: Boolean = false,
    keyType: Boolean = false
) extends TypeInformation[T] {

  val typeClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  val serializer: TypeSerializer[T] = implicitly[TypeSerializer[T]]

  override def createSerializer(config: SerializerConfig): TypeSerializer[T] = serializer.duplicate()
  // override modifier removed to satisfy both implementation requirement of Flink 1.x and removal in 2.x
  def createSerializer(config: ExecutionConfig): TypeSerializer[T] = serializer.duplicate()

  override def isBasicType: Boolean   = basicType
  override def isTupleType: Boolean   = tupleType
  override def isKeyType: Boolean     = keyType
  override def getTotalFields: Int    = totalFields
  override def getTypeClass: Class[T] = typeClass
  override def getArity: Int          = arity

}
