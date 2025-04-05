package org.apache.flink.streaming.util.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.util.Preconditions.checkNotNull

class DefaultScalaProductFieldAccessorFactory extends ScalaProductFieldAccessorFactory with Serializable {

  override def createSimpleProductFieldAccessor[T, F](
      pos: Int,
      typeInfo: TypeInformation[T],
      config: ExecutionConfig
  ): FieldAccessor[T, F] =
    new SimpleProductFieldAccessor[T, F](pos, typeInfo, config)

  override def createRecursiveProductFieldAccessor[T, R, F](
      pos: Int,
      typeInfo: TypeInformation[T],
      innerAccessor: FieldAccessor[R, F],
      config: ExecutionConfig
  ): FieldAccessor[T, F] =
    new RecursiveProductFieldAccessor[T, R, F](pos, typeInfo, innerAccessor, config)

  @SerialVersionUID(1L)
  final class SimpleProductFieldAccessor[T, F] private[typeutils] (
      private val pos: Int,
      typeInfo: TypeInformation[T],
      config: ExecutionConfig
  ) extends FieldAccessor[T, F] {
    checkNotNull(typeInfo, "typeInfo must not be null.")
    private val arity = typeInfo.getArity

    if (pos < 0 || pos >= arity)
      throw new CompositeType.InvalidFieldReferenceException(
        s"""Tried to select $pos. field on "$typeInfo", which is an invalid index."""
      )

    fieldType = typeInfo.asInstanceOf[TupleTypeInfoBase[T]].getTypeAt(pos)

    private val serializer = typeInfo.createSerializer(config).asInstanceOf[TupleSerializerBase[T]]
    private val length     = serializer.getArity
    private val fields     = new Array[AnyRef](length)

    override def get(record: T): F = {
      val prod = record.asInstanceOf[Product]
      prod.productElement(pos).asInstanceOf[F]
    }

    override def set(record: T, fieldValue: F): T = {
      val prod = record.asInstanceOf[Product]
      for (i <- 0 until length) {
        fields.update(i, prod.productElement(i).asInstanceOf[AnyRef])
      }
      fields(pos) = fieldValue.asInstanceOf[AnyRef]
      serializer.createInstance(fields)
    }
  }

  @SerialVersionUID(1L)
  final class RecursiveProductFieldAccessor[T, R, F] private[typeutils] (
      private val pos: Int,
      typeInfo: TypeInformation[T],
      private val innerAccessor: FieldAccessor[R, F],
      config: ExecutionConfig
  ) extends FieldAccessor[T, F] {
    checkNotNull(typeInfo, "typeInfo must not be null.")
    private val arity = typeInfo.getArity

    if (pos < 0 || pos >= arity)
      throw new CompositeType.InvalidFieldReferenceException(
        s"""Tried to select ${pos.asInstanceOf[Integer]}. field on "$typeInfo", which is an invalid index."""
      )
    checkNotNull(innerAccessor, "innerAccessor must not be null.")

    fieldType = innerAccessor.getFieldType

    private val serializer = typeInfo.createSerializer(config).asInstanceOf[TupleSerializerBase[T]]
    private val length     = serializer.getArity
    private val fields     = new Array[AnyRef](length)

    override def get(record: T): F =
      innerAccessor.get(record.asInstanceOf[Product].productElement(pos).asInstanceOf[R])

    override def set(record: T, fieldValue: F): T = {
      val prod = record.asInstanceOf[Product]
      for (i <- 0 until length) {
        fields(i) = prod.productElement(i).asInstanceOf[AnyRef]
      }
      fields(pos) = innerAccessor.set(fields(pos).asInstanceOf[R], fieldValue).asInstanceOf[AnyRef]
      serializer.createInstance(fields)
    }
  }
}
