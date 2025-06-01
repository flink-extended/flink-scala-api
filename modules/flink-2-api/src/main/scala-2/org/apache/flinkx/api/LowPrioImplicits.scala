package org.apache.flinkx.api

import magnolia1.{CaseClass, Magnolia, SealedTrait}
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.serializer.{CoproductSerializer, CaseClassSerializer, ScalaCaseObjectSerializer}
import org.apache.flinkx.api.typeinfo.{CoproductTypeInformation, ProductTypeInformation}
import org.apache.flinkx.api.util.ClassUtil.isFieldFinal

import scala.collection.mutable
import scala.language.experimental.macros
import scala.reflect._
import scala.reflect.runtime.universe.{Try => _, _}

private[api] trait LowPrioImplicits {
  type Typeclass[T] = TypeInformation[T]

  protected def config: SerializerConfig

  protected def cache: mutable.Map[String, TypeInformation[_]]

  def join[T <: Product: ClassTag: TypeTag](
      ctx: CaseClass[TypeInformation, T]
  ): TypeInformation[T] = {
    val cacheKey = typeName[T]
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        val clazz      = classTag[T].runtimeClass.asInstanceOf[Class[T]]
        val serializer = if (typeOf[T].typeSymbol.isModuleClass) {
          new ScalaCaseObjectSerializer[T](clazz)
        } else {
          val fields = clazz.getDeclaredFields
          new CaseClassSerializer[T](
            clazz = clazz,
            scalaFieldSerializers = ctx.parameters.map(_.typeclass.createSerializer(config)).toArray,
            isCaseClassImmutable = ctx.parameters.forall(p => isFieldFinal(fields, clazz.getName, p.label))
          )
        }
        val ti = new ProductTypeInformation[T](
          c = clazz,
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

  implicit def deriveTypeInformation[T]: TypeInformation[T] = macro Magnolia.gen[T]
}
