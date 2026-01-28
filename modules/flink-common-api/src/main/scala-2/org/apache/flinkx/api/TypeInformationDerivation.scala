package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer, ScalaCaseObjectSerializer, nullable}
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation, MarkerTypeInfo}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.collection.mutable
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.{ClassTag, classTag}

private[api] trait TypeInformationDerivation {

  private[api] type Typeclass[T] = TypeInformation[T]

  private val config: SerializerConfig = new SerializerConfigImpl()

  protected val cache: mutable.Map[String, TypeInformation[_]] = mutable.Map.empty

  def join[T <: Product: ClassTag: TypeTag](
      ctx: CaseClass[TypeInformation, T]
  ): TypeInformation[T] = {
    val cacheKey = typeName[T]
    cache.get(cacheKey) match {
      case Some(MarkerTypeInfo) =>
        throw new FlinkRuntimeException(s"Unsupported: recursivity detected in '$cacheKey'.")
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        cache.put(cacheKey, MarkerTypeInfo)
        val clazz      = classTag[T].runtimeClass.asInstanceOf[Class[T]]
        val serializer = if (typeOf[T].typeSymbol.isModuleClass) {
          new ScalaCaseObjectSerializer[T](clazz)
        } else {
          new CaseClassSerializer[T](
            clazz = clazz,
            scalaFieldSerializers = ctx.parameters.map { p =>
              val ser = p.typeclass.createSerializer(config)
              if (p.annotations.exists(_.isInstanceOf[nullable])) {
                NullableSerializer.wrapIfNullIsNotSupported(ser, true)
              } else ser
            }.toArray,
            isCaseClassImmutable = isCaseClassImmutable(clazz, ctx.parameters.map(_.label))
          )
        }
        val ti = new CaseClassTypeInfo[T](
          clazz = clazz,
          fieldTypes = ctx.parameters.map(_.typeclass),
          fieldNames = ctx.parameters.map(_.label),
          ser = serializer
        )
        cache.put(cacheKey, ti)
        ti
    }
  }

  def split[T: ClassTag: TypeTag](ctx: SealedTrait[TypeInformation, T]): TypeInformation[T] = {
    val cacheKey = typeName[T]
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        val serializer = new CoproductSerializer[T](
          subtypeClasses = ctx.subtypes.map(_.typeclass.getTypeClass).toArray,
          subtypeSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
        )
        val clazz = classTag[T].runtimeClass.asInstanceOf[Class[T]]
        val ti    = new CoproductTypeInformation[T](clazz, serializer)
        cache.put(cacheKey, ti)
        ti
    }
  }

  private def typeName[T: TypeTag]: String = typeOf[T].toString

}
