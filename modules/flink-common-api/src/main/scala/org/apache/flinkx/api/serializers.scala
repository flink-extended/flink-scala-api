package org.apache.flinkx.api

import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, LocalTimeTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.{
  BooleanSerializer,
  ByteSerializer,
  CharSerializer,
  DoubleSerializer,
  FloatSerializer,
  IntSerializer,
  LongSerializer,
  ShortSerializer
}
import org.apache.flink.api.common.typeutils.base.array._
import org.apache.flink.api.java.typeutils.EnumTypeInfo
import org.apache.flink.types.{Nothing => FlinkNothing}
import org.apache.flinkx.api.mapper.{BigDecMapper, BigIntMapper, UuidMapper}
import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper
import org.apache.flinkx.api.serializer._
import org.apache.flinkx.api.typeinfo._

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
import java.time._
import java.util.UUID
import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.duration.{Duration, FiniteDuration, TimeUnit}
import scala.reflect.{ClassTag, classTag}

trait serializers extends LowPrioImplicits {

  override protected val config: SerializerConfig = new SerializerConfigImpl()

  implicit def infoToSer[T](implicit ti: TypeInformation[T]): TypeSerializer[T] = ti.createSerializer(config)

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
  implicit def mapSerializer[K: ClassTag, V: ClassTag](implicit
      ks: TypeSerializer[K],
      vs: TypeSerializer[V]
  ): TypeSerializer[Map[K, V]] =
    new MapSerializer[K, V](ks, vs)

  implicit def seqSerializer[T: ClassTag](implicit vs: TypeSerializer[T]): TypeSerializer[Seq[T]] =
    new SeqSerializer[T](vs, classTag[T].runtimeClass.asInstanceOf[Class[T]])

  implicit def eitherSerializer[L: ClassTag, R: ClassTag](implicit
      ls: TypeSerializer[L],
      rs: TypeSerializer[R]
  ): EitherSerializer[L, R] =
    new EitherSerializer[L, R](ls, rs)

  implicit val intArraySerializer: TypeSerializer[Array[Int]]         = new IntPrimitiveArraySerializer()
  implicit val longArraySerializer: TypeSerializer[Array[Long]]       = new LongPrimitiveArraySerializer()
  implicit val floatArraySerializer: TypeSerializer[Array[Float]]     = new FloatPrimitiveArraySerializer()
  implicit val doubleArraySerializer: TypeSerializer[Array[Double]]   = new DoublePrimitiveArraySerializer()
  implicit val booleanArraySerializer: TypeSerializer[Array[Boolean]] = new BooleanPrimitiveArraySerializer()
  implicit val byteArraySerializer: TypeSerializer[Array[Byte]]       = new BytePrimitiveArraySerializer()
  implicit val charArraySerializer: TypeSerializer[Array[Char]]       = new CharPrimitiveArraySerializer()
  implicit val shortArraySerializer: TypeSerializer[Array[Short]]     = new ShortPrimitiveArraySerializer()
  implicit val stringArraySerializer: TypeSerializer[Array[String]]   = new StringArraySerializer()

  implicit lazy val jIntegerSerializer: TypeSerializer[Integer]               = new IntSerializer()
  implicit lazy val jLongSerializer: TypeSerializer[JLong]                    = new LongSerializer()
  implicit lazy val jFloatSerializer: TypeSerializer[JFloat]                  = new FloatSerializer()
  implicit lazy val jDoubleSerializer: TypeSerializer[JDouble]                = new DoubleSerializer()
  implicit lazy val jBooleanSerializer: TypeSerializer[JBoolean]              = new BooleanSerializer()
  implicit lazy val jByteSerializer: TypeSerializer[JByte]                    = new ByteSerializer()
  implicit lazy val jCharSerializer: TypeSerializer[JCharacter]               = new CharSerializer()
  implicit lazy val jShortSerializer: TypeSerializer[JShort]                  = new ShortSerializer()
  implicit lazy val jZoneIdSerializer: TypeSerializer[ZoneId]                 = ZoneIdSerializer
  implicit lazy val jZoneOffsetSerializer: TypeSerializer[ZoneOffset]         = ZoneOffsetSerializer
  implicit lazy val jZonedDateTimeSerializer: TypeSerializer[ZonedDateTime]   = ZonedDateTimeSerializer
  implicit lazy val jOffsetDateTimeSerializer: TypeSerializer[OffsetDateTime] = OffsetDateTimeSerializer
  implicit lazy val durationSerializer: TypeSerializer[Duration]              = DurationSerializer
  implicit lazy val finiteDurationSerializer: TypeSerializer[FiniteDuration]  = FiniteDurationSerializer

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
  implicit lazy val durationInfo: TypeInformation[Duration]             = SimpleTypeInfo(0, keyType = true)
  implicit lazy val finiteDurationInfo: TypeInformation[FiniteDuration] = SimpleTypeInfo(2, 2, keyType = true)
  implicit lazy val flinkNothingInfo: TypeInformation[FlinkNothing]     =
    SimpleTypeInfo(0)(classTag[FlinkNothing], new NothingSerializer().asInstanceOf[TypeSerializer[FlinkNothing]])
  implicit lazy val scalaNothingInfo: TypeInformation[scala.Nothing]  = ScalaNothingTypeInfo
  implicit lazy val timeUnitInfo: TypeInformation[TimeUnit]           = new EnumTypeInfo(classOf[TimeUnit])
  implicit lazy val uuidMapper: TypeMapper[UUID, Array[Byte]]         = new UuidMapper()
  implicit lazy val uuidInfo: TypeInformation[UUID]                   = mappedTypeInfo[UUID, Array[Byte]]
  implicit lazy val unitInfo: TypeInformation[Unit]                   = new UnitTypeInformation()
  implicit lazy val UnitOrderingInfo: TypeInformation[Ordering[Unit]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Unit]], UnitOrderingSerializer)

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
  implicit lazy val jZoneIdInfo: TypeInformation[ZoneId]                   = SimpleTypeInfo(0)
  implicit lazy val jZoneOffsetInfo: TypeInformation[ZoneOffset]           = SimpleTypeInfo(3, 9, keyType = true)
  implicit lazy val jZonedDateTimeInfo: TypeInformation[ZonedDateTime]     = SimpleTypeInfo(3, 11, keyType = true)
  implicit lazy val jOffsetDateTimeInfo: TypeInformation[OffsetDateTime]   = SimpleTypeInfo(2, 10, keyType = true)

  implicit def listCCInfo[T: ClassTag](implicit ls: TypeSerializer[::[T]]): TypeInformation[::[T]] =
    new CollectionTypeInformation[::[T]](ls)

  implicit def listInfo[T: ClassTag](implicit ls: TypeSerializer[List[T]]): TypeInformation[List[T]] =
    new CollectionTypeInformation[List[T]](ls)

  implicit def seqInfo[T: ClassTag](implicit ls: TypeSerializer[Seq[T]]): TypeInformation[Seq[T]] =
    new CollectionTypeInformation[Seq[T]](ls)

  implicit def vectorInfo[T: ClassTag](implicit ls: TypeSerializer[Vector[T]]): TypeInformation[Vector[T]] =
    new CollectionTypeInformation[Vector[T]](ls)

  implicit def setInfo[T: ClassTag](implicit ls: TypeSerializer[Set[T]]): TypeInformation[Set[T]] =
    new CollectionTypeInformation[Set[T]](ls)

  implicit def arrayInfo[T: ClassTag](implicit ls: TypeSerializer[Array[T]]): TypeInformation[Array[T]] =
    new CollectionTypeInformation[Array[T]](ls)

  implicit def mapInfo[K: ClassTag, V: ClassTag](implicit ms: TypeSerializer[Map[K, V]]): TypeInformation[Map[K, V]] =
    new CollectionTypeInformation[Map[K, V]](ms)

  implicit def optionInfo[T](implicit ls: TypeInformation[T]): TypeInformation[Option[T]] =
    new OptionTypeInfo[T, Option[T]](ls)

  implicit def eitherInfo[A, B](implicit
      tag: ClassTag[Either[A, B]],
      a: TypeInformation[A],
      b: TypeInformation[B]
  ): TypeInformation[Either[A, B]] =
    new EitherTypeInfo(tag.runtimeClass.asInstanceOf[Class[Either[A, B]]], a, b)

  /** Create a [[TypeInformation]] of `SortedSet[A]`. Given the fact ordering used by the `SortedSet` cannot be known,
    * the `TypeInformation` of its ordering has to be available in the context.
    * @param as
    *   the serializer of `A`
    * @param aos
    *   the serializer of the `Ordering[A]` used by the `SortedSet`. Must be available in the context
    * @tparam A
    *   the type of the elements contained in the `SortedSet`
    * @return
    *   a `TypeInformation[SortedSet[A]]`
    */
  implicit def sortedSetInfo[A: ClassTag](implicit
      as: TypeSerializer[A],
      aos: TypeSerializer[Ordering[A]]
  ): TypeInformation[SortedSet[A]] = {
    implicit val sortedSetSerializer: SortedSetSerializer[A] =
      new SortedSetSerializer[A](as, classTag[A].runtimeClass.asInstanceOf[Class[A]], aos)
    SimpleTypeInfo[SortedSet[A]](0) // Traits don't have fields
  }

  /** Create a [[TypeInformation]] of `TreeSet[A]`. Given the fact ordering used by the `SortedSet` cannot be known, the
    * `TypeInformation` of its ordering has to be available in the context.
    * @param as
    *   the serializer of `A`
    * @param aos
    *   the serializer of the `Ordering[A]` used by the `TreeSet`. Must be available in the context
    * @tparam A
    *   the type of the elements contained in the `TreeSet`
    * @return
    *   a `TypeInformation[TreeSet[A]]`
    */
  implicit def treeSetInfo[A: ClassTag](implicit
      as: TypeSerializer[A],
      aos: TypeSerializer[Ordering[A]]
  ): TypeInformation[TreeSet[A]] = {
    implicit val sortedSetSerializer: TypeSerializer[TreeSet[A]] =
      new SortedSetSerializer[A](as, classTag[A].runtimeClass.asInstanceOf[Class[A]], aos)
        .asInstanceOf[TypeSerializer[TreeSet[A]]]
    SimpleTypeInfo[TreeSet[A]](2, 2)
  }

}

object serializers extends serializers
