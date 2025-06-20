package org.apache.flinkx.api

import scala.collection.mutable
import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.reflect.ClassTag

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flinkx.api.serializer.{CoproductSerializer, CaseClassSerializer, ScalaCaseObjectSerializer}
import org.apache.flinkx.api.typeinfo.{CoproductTypeInformation, ProductTypeInformation}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

private[api] trait LowPrioImplicits extends TaggedDerivation[TypeInformation]:
  type Typeclass[T] = TypeInformation[T]

  protected def config: SerializerConfig

  protected def cache: mutable.Map[String, TypeInformation[?]]

  // We cannot add a constraint of `T <: Product`, even though `join` is always called on products.
  // Need to mix in via `& Product`.
  override def join[T](ctx: CaseClass[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): Typeclass[T] =
    val useCache = typeTag.isCachable
    val cacheKey = typeTag.toString
    (if useCache then cache.get(cacheKey) else None) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val clazz      = classTag.runtimeClass.asInstanceOf[Class[T & Product]]
        val serializer =
          if typeTag.isModule then new ScalaCaseObjectSerializer[T & Product](clazz)
          else
            new CaseClassSerializer[T & Product](
              clazz = clazz,
              scalaFieldSerializers =
                IArray.genericWrapArray(ctx.params.map(_.typeclass.createSerializer(config))).toArray,
              isCaseClassImmutable = isCaseClassImmutable(clazz, ctx.params.map(_.label))
            )
        val ti = new ProductTypeInformation[T & Product](
          c = clazz,
          fieldTypes = ctx.params.map(_.typeclass),
          fieldNames = ctx.params.map(_.label),
          ser = serializer
        ).asInstanceOf[TypeInformation[T]]
        if useCache then cache.put(cacheKey, ti)
        ti

  override def split[T](ctx: SealedTrait[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): Typeclass[T] =
    val useCache = typeTag.isCachable
    val cacheKey = typeTag.toString
    (if useCache then cache.get(cacheKey) else None) match
      case Some(cached) =>
        cached.asInstanceOf[TypeInformation[T]]

      case None =>
        val serializer = new CoproductSerializer[T](
          subtypeClasses = IArray.genericWrapArray(ctx.subtypes.map(_.typeclass.getTypeClass)).toArray,
          subtypeSerializers = IArray.genericWrapArray(ctx.subtypes.map(_.typeclass.createSerializer(config))).toArray
        )
        val clazz = classTag.runtimeClass.asInstanceOf[Class[T]]
        val ti    = new CoproductTypeInformation[T](clazz, serializer)
        if useCache then cache.put(cacheKey, ti)
        ti

  final inline implicit def deriveTypeInformation[T](implicit
      m: Mirror.Of[T],
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): TypeInformation[T] = derived
