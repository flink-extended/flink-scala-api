package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer, ScalaCaseObjectSerializer, nullable}
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.collection.mutable
import scala.compiletime.summonInline
import scala.reflect.ClassTag

private[api] trait TypeInformationDerivation extends TaggedDerivation[TypeInformation]:

  private[api] type Typeclass[T] = TypeInformation[T]

  private val config: SerializerConfig = new SerializerConfigImpl()

  protected val cache: mutable.Map[String, TypeInformation[?]] = mutable.Map.empty

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
              scalaFieldSerializers = IArray
                .genericWrapArray(ctx.params.map { p =>
                  val ser = p.typeclass.createSerializer(config)
                  if (p.annotations.exists(_.isInstanceOf[nullable])) {
                    NullableSerializer.wrapIfNullIsNotSupported(ser, true)
                  } else ser
                })
                .toArray,
              isCaseClassImmutable = isCaseClassImmutable(clazz, ctx.params.map(_.label))
            )
        val ti = new CaseClassTypeInfo[T & Product](
          clazz = clazz,
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
