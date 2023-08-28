package org.apache.flinkx.api

import org.apache.flinkx.api.serializer.{CoproductSerializer, ScalaCaseClassSerializer, ScalaCaseObjectSerializer}
import org.apache.flinkx.api.typeinfo.{CoproductTypeInformation, ProductTypeInformation}
import magnolia1.{CaseClass, Magnolia, SealedTrait}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.experimental.macros
import scala.reflect._
import scala.reflect.runtime.universe.{Try => _, _}
import scala.util.{Failure, Success, Try}

private[api] trait LowPrioImplicits {
  type Typeclass[T] = TypeInformation[T]

  protected def config: ExecutionConfig

  protected def cache: mutable.Map[String, TypeInformation[_]]

  def join[T <: Product: ClassTag: TypeTag](
      ctx: CaseClass[TypeInformation, T]
  ): TypeInformation[T] = {
    val cacheKey = typeName(ctx.typeName)
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None =>
        val clazz = classTag[T].runtimeClass.asInstanceOf[Class[T]]
        val serializer = if (typeOf[T].typeSymbol.isModuleClass) {
          new ScalaCaseObjectSerializer[T](clazz)
        } else {
          new ScalaCaseClassSerializer[T](
            clazz = clazz,
            scalaFieldSerializers = ctx.parameters.map(_.typeclass.createSerializer(config)).toArray
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

  def split[T: ClassTag](ctx: SealedTrait[TypeInformation, T]): TypeInformation[T] = {
    val cacheKey = typeName(ctx.typeName)
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None =>
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

  private def typeName(tn: magnolia1.TypeName): String =
    s"${tn.full}[${tn.typeArguments.map(typeName).mkString(",")}]"

  implicit def deriveTypeInformation[T]: TypeInformation[T] = macro Magnolia.gen[T]
}
