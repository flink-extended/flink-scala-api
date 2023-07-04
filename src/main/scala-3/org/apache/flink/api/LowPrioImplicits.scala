package org.apache.flink.api

import scala.collection.mutable
import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.reflect.ClassTag

import magnolia1.{CaseClass, SealedTrait, TypeInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.ExecutionConfig

import org.apache.flink.api.serializer.{CoproductSerializer, ScalaCaseClassSerializer, ScalaCaseObjectSerializer}
import org.apache.flink.api.typeinfo.{CoproductTypeInformation, ProductTypeInformation}

private[api] trait LowPrioImplicits extends TaggedDerivation[TypeInformation]:
  type Typeclass[T] = TypeInformation[T]

  protected def config: ExecutionConfig

  protected def cache: mutable.Map[String, TypeInformation[?]]

  // We cannot add a constraint of `T <: Product`, even though `join` is always called on products.
  // Need to mix in via `& Product`.
  override def join[T](ctx: CaseClass[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): Typeclass[T] =
    val cacheKey = typeName(ctx.typeInfo)
    cache.get(cacheKey) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz = classTag.runtimeClass.asInstanceOf[Class[T & Product]]
        val serializer =
          if typeTag.isModule then new ScalaCaseObjectSerializer[T & Product](clazz)
          else
            new ScalaCaseClassSerializer[T & Product](
              clazz = clazz,
              scalaFieldSerializers =
                IArray.genericWrapArray(ctx.params.map(_.typeclass.createSerializer(config))).toArray
            )
        val ti = new ProductTypeInformation[T & Product](
          c = clazz,
          fieldTypes = ctx.params.map(_.typeclass),
          fieldNames = ctx.params.map(_.label),
          ser = serializer
        ).asInstanceOf[TypeInformation[T]]
        cache.put(cacheKey, ti)
        ti

  override def split[T](ctx: SealedTrait[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): Typeclass[T] =
    val cacheKey = typeName(ctx.typeInfo)
    cache.get(cacheKey) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val serializer = new CoproductSerializer[T](
          subtypeClasses = IArray.genericWrapArray(ctx.subtypes.map(_.typeclass.getTypeClass)).toArray,
          subtypeSerializers = IArray.genericWrapArray(ctx.subtypes.map(_.typeclass.createSerializer(config))).toArray
        )
        val clazz = classTag.runtimeClass.asInstanceOf[Class[T]]
        val ti    = new CoproductTypeInformation[T](clazz, serializer)
        cache.put(cacheKey, ti)
        ti

  final inline implicit def deriveTypeInformation[T](implicit
      m: Mirror.Of[T],
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): TypeInformation[T] = derived

  private def typeName(ti: TypeInfo): String =
    s"${ti.full}[${ti.typeParams.map(typeName).mkString(",")}]"
