package org.apache.flinkx.api

import org.apache.flinkx.api.mapper.{BigDecMapper, BigIntMapper, UuidMapper}
import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper
import org.apache.flinkx.api.serializer._
import org.apache.flinkx.api.typeinfo._
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, LocalTimeTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.array._

import java.lang.{Float => JFloat}
import java.lang.{Long => JLong}
import java.lang.{Double => JDouble}
import java.lang.{Short => JShort}
import java.lang.{Byte => JByte}
import java.lang.{Boolean => JBoolean}
import java.lang.{Integer => JInteger}
import java.lang.{Character => JCharacter}
import java.math.{BigInteger => JBigInteger}
import java.math.{BigDecimal => JBigDecimal}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID
import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}

object serializers extends LowPrioImplicits {
  override protected val config: SerializerConfig = new SerializerConfigImpl(new Configuration())

  override protected val cache: mutable.Map[String, Typeclass[_]] = mutable.Map[String, TypeInformation[_]]()

  implicit def into2ser[T](implicit ti: TypeInformation[T]): TypeSerializer[T] = ti.createSerializer(config)

  implicit def optionSerializer[T](implicit vs: TypeSerializer[T]): TypeSerializer[Option[T]] =
    new OptionSerializer[T](vs)
  implicit def listSerializer[T: ClassTag](implicit vs: TypeSerializer[T]): TypeSerializer[List[T]] =
    new ListSerializer[T](vs, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  implicit def listCCSerializer[T: ClassTag](implicit vs: TypeSerializer[T]): TypeSerializer[::[T]] =
    new ListCCSerializer[T](vs, classTag[T].runtimeClass.asInstanceOf[Class[T]])

  implicit def vectorSerializer[T: ClassTag](implicit vs: TypeSerializer[T]): TypeSerializer[Vector[T]] =
    new VectorSerializer[T](vs, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  implicit def arraySerializer[T: ClassTag](implicit vs: TypeSerializer[T]): TypeSerializer[Array[T]] =
    new ArraySerializer[T](vs, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  implicit def setSerializer[T: ClassTag](implicit vs: TypeSerializer[T]): TypeSerializer[Set[T]] =
    new SetSerializer[T](vs, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  implicit def mapSerializer[K, V](implicit
      kc: ClassTag[K],
      vc: ClassTag[V],
      ks: TypeSerializer[K],
      vs: TypeSerializer[V]
  ): TypeSerializer[Map[K, V]] = {
    drop(kc)
    drop(vc)
    new MapSerializer[K, V](ks, vs)
  }

  implicit def seqSerializer[T: ClassTag](implicit vs: TypeSerializer[T]): TypeSerializer[Seq[T]] =
    new SeqSerializer[T](vs, classTag[T].runtimeClass.asInstanceOf[Class[T]])

  implicit def eitherSerializer[L, R](implicit
      lc: ClassTag[L],
      rc: ClassTag[R],
      ls: TypeSerializer[L],
      rs: TypeSerializer[R]
  ): EitherSerializer[L, R] = {
    drop(lc)
    drop(rc)
    new EitherSerializer[L, R](ls, rs)
  }

  implicit val intArraySerializer: TypeSerializer[Array[Int]]         = new IntPrimitiveArraySerializer()
  implicit val longArraySerializer: TypeSerializer[Array[Long]]       = new LongPrimitiveArraySerializer()
  implicit val floatArraySerializer: TypeSerializer[Array[Float]]     = new FloatPrimitiveArraySerializer()
  implicit val doubleArraySerializer: TypeSerializer[Array[Double]]   = new DoublePrimitiveArraySerializer()
  implicit val booleanArraySerializer: TypeSerializer[Array[Boolean]] = new BooleanPrimitiveArraySerializer()
  implicit val byteArraySerializer: TypeSerializer[Array[Byte]]       = new BytePrimitiveArraySerializer()
  implicit val charArraySerializer: TypeSerializer[Array[Char]]       = new CharPrimitiveArraySerializer()
  implicit val shortArraySerializer: TypeSerializer[Array[Short]]     = new ShortPrimitiveArraySerializer()
  implicit val stringArraySerializer: TypeSerializer[Array[String]]   = new StringArraySerializer()

  implicit lazy val jIntegerSerializer: TypeSerializer[Integer] =
    new org.apache.flink.api.common.typeutils.base.IntSerializer()
  implicit lazy val jLongSerializer: TypeSerializer[JLong] =
    new org.apache.flink.api.common.typeutils.base.LongSerializer()
  implicit lazy val jFloatSerializer: TypeSerializer[JFloat] =
    new org.apache.flink.api.common.typeutils.base.FloatSerializer()
  implicit lazy val jDoubleSerializer: TypeSerializer[JDouble] =
    new org.apache.flink.api.common.typeutils.base.DoubleSerializer()
  implicit lazy val jBooleanSerializer: TypeSerializer[JBoolean] =
    new org.apache.flink.api.common.typeutils.base.BooleanSerializer()
  implicit lazy val jByteSerializer: TypeSerializer[JByte] =
    new org.apache.flink.api.common.typeutils.base.ByteSerializer()
  implicit lazy val jCharSerializer: TypeSerializer[JCharacter] =
    new org.apache.flink.api.common.typeutils.base.CharSerializer()
  implicit lazy val jShortSerializer: TypeSerializer[JShort] =
    new org.apache.flink.api.common.typeutils.base.ShortSerializer()

  // type infos
  implicit lazy val stringInfo: TypeInformation[String] = BasicTypeInfo.STRING_TYPE_INFO
  implicit lazy val intInfo: TypeInformation[Int]       = BasicTypeInfo.getInfoFor(classOf[Int])
  implicit lazy val boolInfo: TypeInformation[Boolean]  = BasicTypeInfo.getInfoFor(classOf[Boolean])
  implicit lazy val byteInfo: TypeInformation[Byte]     = BasicTypeInfo.getInfoFor(classOf[Byte])
  implicit lazy val charInfo: TypeInformation[Char]     = BasicTypeInfo.getInfoFor(classOf[Char])
  implicit lazy val doubleInfo: TypeInformation[Double] = BasicTypeInfo.getInfoFor(classOf[Double])
  implicit lazy val floatInfo: TypeInformation[Float]   = BasicTypeInfo.getInfoFor(classOf[Float])
  implicit lazy val longInfo: TypeInformation[Long]     = BasicTypeInfo.getInfoFor(classOf[Long])
  implicit lazy val shortInfo: TypeInformation[Short]   = BasicTypeInfo.getInfoFor(classOf[Short])

  implicit lazy val bigDecMapper: TypeMapper[scala.BigDecimal, JBigDecimal] = new BigDecMapper()
  implicit lazy val bigDecInfo: TypeInformation[scala.BigDecimal]       = mappedTypeInfo[scala.BigDecimal, JBigDecimal]
  implicit lazy val bigIntMapper: TypeMapper[scala.BigInt, JBigInteger] = new BigIntMapper()
  implicit lazy val bigIntInfo: TypeInformation[BigInt]                 = mappedTypeInfo[scala.BigInt, JBigInteger]
  implicit lazy val uuidMapper: TypeMapper[UUID, Array[Byte]]           = new UuidMapper()
  implicit lazy val uuidInfo: TypeInformation[UUID]                     = mappedTypeInfo[UUID, Array[Byte]]

  implicit lazy val unitInfo: TypeInformation[Unit] = new UnitTypeInformation()
  implicit def mappedTypeInfo[A: ClassTag, B](implicit
      mapper: TypeMapper[A, B],
      ti: TypeInformation[B]
  ): TypeInformation[A] =
    new MappedTypeInformation[A, B](mapper, ti)

  // serializers
  implicit lazy val stringSerializer: TypeSerializer[String]   = stringInfo.createSerializer(config)
  implicit lazy val intSerializer: TypeSerializer[Int]         = intInfo.createSerializer(config)
  implicit lazy val longSerializer: TypeSerializer[Long]       = longInfo.createSerializer(config)
  implicit lazy val floatSerializer: TypeSerializer[Float]     = floatInfo.createSerializer(config)
  implicit lazy val doubleSerializer: TypeSerializer[Double]   = doubleInfo.createSerializer(config)
  implicit lazy val booleanSerializer: TypeSerializer[Boolean] = boolInfo.createSerializer(config)
  implicit lazy val byteSerializer: TypeSerializer[Byte]       = byteInfo.createSerializer(config)
  implicit lazy val charSerializer: TypeSerializer[Char]       = charInfo.createSerializer(config)
  implicit lazy val shortSerializer: TypeSerializer[Short]     = shortInfo.createSerializer(config)

  // java
  implicit lazy val jIntegerInfo: TypeInformation[JInteger]                = BasicTypeInfo.INT_TYPE_INFO
  implicit lazy val jLongInfo: TypeInformation[JLong]                      = BasicTypeInfo.LONG_TYPE_INFO
  implicit lazy val jFloatInfo: TypeInformation[JFloat]                    = BasicTypeInfo.FLOAT_TYPE_INFO
  implicit lazy val jDoubleInfo: TypeInformation[JDouble]                  = BasicTypeInfo.DOUBLE_TYPE_INFO
  implicit lazy val jBooleanInfo: TypeInformation[JBoolean]                = BasicTypeInfo.BOOLEAN_TYPE_INFO
  implicit lazy val jByteInfo: TypeInformation[JByte]                      = BasicTypeInfo.BYTE_TYPE_INFO
  implicit lazy val jCharInfo: TypeInformation[JCharacter]                 = BasicTypeInfo.CHAR_TYPE_INFO
  implicit lazy val jShortInfo: TypeInformation[JShort]                    = BasicTypeInfo.SHORT_TYPE_INFO
  implicit lazy val jVoidInfo: TypeInformation[java.lang.Void]             = BasicTypeInfo.VOID_TYPE_INFO
  implicit lazy val jBigIntInfo: TypeInformation[JBigInteger]              = BasicTypeInfo.BIG_INT_TYPE_INFO
  implicit lazy val jBigDecInfo: TypeInformation[JBigDecimal]              = BasicTypeInfo.BIG_DEC_TYPE_INFO
  implicit lazy val jInstantInfo: TypeInformation[Instant]                 = BasicTypeInfo.INSTANT_TYPE_INFO
  implicit lazy val jLocalDateTypeInfo: TypeInformation[LocalDate]         = LocalTimeTypeInfo.LOCAL_DATE
  implicit lazy val jLocalDateTimeTypeInfo: TypeInformation[LocalDateTime] = LocalTimeTypeInfo.LOCAL_DATE_TIME
  implicit lazy val jLocalTimeTypeInfo: TypeInformation[LocalTime]         = LocalTimeTypeInfo.LOCAL_TIME

  implicit def listCCInfo[T](implicit lc: ClassTag[T], ls: TypeSerializer[::[T]]): TypeInformation[::[T]] = {
    drop(lc)
    new CollectionTypeInformation[::[T]](ls)
  }

  implicit def listInfo[T](implicit
      lc: ClassTag[T],
      ls: TypeSerializer[List[T]]
  ): TypeInformation[List[T]] = {
    drop(lc)
    new CollectionTypeInformation[List[T]](ls)
  }

  implicit def seqInfo[T](implicit lc: ClassTag[T], ls: TypeSerializer[Seq[T]]): TypeInformation[Seq[T]] = {
    drop(lc)
    new CollectionTypeInformation[Seq[T]](ls)
  }

  implicit def vectorInfo[T](implicit
      lc: ClassTag[T],
      ls: TypeSerializer[Vector[T]]
  ): TypeInformation[Vector[T]] = {
    drop(lc)
    new CollectionTypeInformation[Vector[T]](ls)
  }

  implicit def setInfo[T](implicit lc: ClassTag[T], ls: TypeSerializer[Set[T]]): TypeInformation[Set[T]] = {
    drop(lc)
    new CollectionTypeInformation[Set[T]](ls)
  }

  implicit def arrayInfo[T](implicit
      lc: ClassTag[T],
      ls: TypeSerializer[Array[T]]
  ): TypeInformation[Array[T]] = {
    drop(lc)
    new CollectionTypeInformation[Array[T]](ls)
  }

  implicit def mapInfo[K, V](implicit
      kc: ClassTag[K],
      vc: ClassTag[V],
      ms: TypeSerializer[Map[K, V]]
  ): TypeInformation[Map[K, V]] = {
    drop(kc)
    drop(vc)
    new CollectionTypeInformation[Map[K, V]](ms)
  }

  implicit def optionInfo[T](implicit ls: TypeInformation[T]): TypeInformation[Option[T]] =
    new OptionTypeInfo[T, Option[T]](ls)

  implicit def eitherInfo[A, B](implicit
      tag: ClassTag[Either[A, B]],
      a: TypeInformation[A],
      b: TypeInformation[B]
  ): TypeInformation[Either[A, B]] =
    new EitherTypeInfo(tag.runtimeClass.asInstanceOf[Class[Either[A, B]]], a, b)

  private[flinkx] def drop[A](a: => A): Unit = {
    val _ = a
    ()
  }
}
