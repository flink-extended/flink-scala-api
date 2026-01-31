package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.semiauto.infoToSer
import org.apache.flinkx.api.typeinfo.SimpleTypeInfo

import scala.math.Ordering.OptionOrdering
import scala.reflect.{ClassTag, classTag}

abstract class ConstantOrderingSerializer[A] extends ImmutableSerializer[Ordering[A]] {
  override def getLength: Int                                                      = 0     // Nothing to serialize
  override def serialize(record: Ordering[A], target: DataOutputView): Unit        = {}    // Nothing to serialize
  override def deserialize(reuse: Ordering[A], source: DataInputView): Ordering[A] = reuse // Can be reused
  override def deserialize(source: DataInputView): Ordering[A]                     = createInstance()
}

object OrderingTypeInfo {
  // Serializers for orderings are not implicitly available in the context because we cannot presume the user want to
  // use them. Additionally, there are already 2 orderings available for Float and Double: default and IEEE ordering.

  lazy val DefaultBooleanOrderingInfo: TypeInformation[Ordering[Boolean]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Boolean]], DefaultBooleanOrderingSerializer)
  lazy val DefaultByteOrderingInfo: TypeInformation[Ordering[Byte]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Byte]], DefaultByteOrderingSerializer)
  lazy val DefaultCharOrderingInfo: TypeInformation[Ordering[Char]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Char]], DefaultCharOrderingSerializer)
  lazy val DefaultShortOrderingInfo: TypeInformation[Ordering[Short]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Short]], DefaultShortOrderingSerializer)
  lazy val DefaultIntOrderingInfo: TypeInformation[Ordering[Int]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Int]], DefaultIntOrderingSerializer)
  lazy val DefaultLongOrderingInfo: TypeInformation[Ordering[Long]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Long]], DefaultLongOrderingSerializer)
  lazy val DefaultFloatOrderingInfo: TypeInformation[Ordering[Float]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Float]], DefaultFloatOrderingSerializer)
  lazy val FloatIeeeOrderingInfo: TypeInformation[Ordering[Float]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Float]], FloatIeeeOrderingSerializer)
  lazy val DefaultDoubleOrderingInfo: TypeInformation[Ordering[Double]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Double]], DefaultDoubleOrderingSerializer)
  lazy val DoubleIeeeOrderingInfo: TypeInformation[Ordering[Double]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[Double]], DoubleIeeeOrderingSerializer)
  lazy val DefaultBigIntOrderingInfo: TypeInformation[Ordering[BigInt]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[BigInt]], DefaultBigIntOrderingSerializer)
  lazy val DefaultBigDecimalOrderingInfo: TypeInformation[Ordering[BigDecimal]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[BigDecimal]], DefaultBigDecimalOrderingSerializer)
  lazy val DefaultStringOrderingInfo: TypeInformation[Ordering[String]] =
    SimpleTypeInfo(keyType = true)(classTag[Ordering[String]], DefaultStringOrderingSerializer)

  def reverse[A](child: TypeInformation[Ordering[A]])(implicit
      childTag: ClassTag[Ordering[A]]
  ): TypeInformation[Ordering[A]] =
    SimpleTypeInfo(keyType = true)(childTag, new ReverseOrderingSerializer[A](infoToSer(child)))

  def Option[A](child: TypeInformation[Ordering[A]])(implicit
      childTag: ClassTag[Ordering[Option[A]]]
  ): TypeInformation[Ordering[Option[A]]] =
    SimpleTypeInfo(keyType = true)(childTag, new OptionOrderingSerializer[A](infoToSer(child)))

  def deriveOrdering[A <: Ordering[B], B](implicit a: TypeInformation[A]): TypeInformation[Ordering[B]] =
    a.asInstanceOf[TypeInformation[Ordering[B]]]
}

// Ordering[Unit]

object UnitOrderingSerializer extends ConstantOrderingSerializer[Unit] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Unit]] = new UnitOrderingSerializerSnapshot
  override def createInstance(): Ordering[Unit]                                = Ordering.Unit
}

class UnitOrderingSerializerSnapshot extends SimpleTypeSerializerSnapshot[Ordering[Unit]](() => UnitOrderingSerializer)

// Ordering[Boolean]

object DefaultBooleanOrderingSerializer extends ConstantOrderingSerializer[Boolean] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Boolean]] =
    new DefaultBooleanOrderingSerializerSnapshot
  override def createInstance(): Ordering[Boolean] = Ordering.Boolean
}

class DefaultBooleanOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Boolean]](() => DefaultBooleanOrderingSerializer)

// Ordering[Byte]

object DefaultByteOrderingSerializer extends ConstantOrderingSerializer[Byte] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Byte]] =
    new DefaultByteOrderingSerializerSnapshot
  override def createInstance(): Ordering[Byte] = Ordering.Byte
}

class DefaultByteOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Byte]](() => DefaultByteOrderingSerializer)

// Ordering[Char]

object DefaultCharOrderingSerializer extends ConstantOrderingSerializer[Char] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Char]] =
    new DefaultCharOrderingSerializerSnapshot
  override def createInstance(): Ordering[Char] = Ordering.Char
}

class DefaultCharOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Char]](() => DefaultCharOrderingSerializer)

// Ordering[Short]

object DefaultShortOrderingSerializer extends ConstantOrderingSerializer[Short] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Short]] =
    new DefaultShortOrderingSerializerSnapshot
  override def createInstance(): Ordering[Short] = Ordering.Short
}

class DefaultShortOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Short]](() => DefaultShortOrderingSerializer)

// Ordering[Int]

object DefaultIntOrderingSerializer extends ConstantOrderingSerializer[Int] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Int]] = new DefaultIntOrderingSerializerSnapshot
  override def createInstance(): Ordering[Int]                                = Ordering.Int
}

class DefaultIntOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Int]](() => DefaultIntOrderingSerializer)

// Ordering[Long]

object DefaultLongOrderingSerializer extends ConstantOrderingSerializer[Long] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Long]] =
    new DefaultLongOrderingSerializerSnapshot
  override def createInstance(): Ordering[Long] = Ordering.Long
}

class DefaultLongOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Long]](() => DefaultLongOrderingSerializer)

// Ordering[Float]

object DefaultFloatOrderingSerializer extends ConstantOrderingSerializer[Float] {
  override def serialize(record: Ordering[Float], target: DataOutputView): Unit =
    if (!record.isInstanceOf[Ordering.Float.TotalOrdering]) {
      throw new IllegalArgumentException(
        s"This serializer can only serialize Ordering.Float.TotalOrdering, not $record"
      )
    }
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Float]] =
    new DefaultFloatOrderingSerializerSnapshot
  override def createInstance(): Ordering[Float] = Ordering.Float.TotalOrdering
}

class DefaultFloatOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Float]](() => DefaultFloatOrderingSerializer)

// Ordering.Float.IeeeOrdering

object FloatIeeeOrderingSerializer extends ConstantOrderingSerializer[Float] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Float]] =
    new FloatIeeeOrderingSerializerSnapshot
  override def createInstance(): Ordering[Float] = Ordering.Float.IeeeOrdering
}

class FloatIeeeOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Float]](() => FloatIeeeOrderingSerializer)

// Ordering[Double]

object DefaultDoubleOrderingSerializer extends ConstantOrderingSerializer[Double] {
  override def serialize(record: Ordering[Double], target: DataOutputView): Unit =
    if (!record.isInstanceOf[Ordering.Double.TotalOrdering]) {
      throw new IllegalArgumentException(
        s"This serializer can only serialize Ordering.Double.TotalOrdering, not $record"
      )
    }
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Double]] =
    new DefaultDoubleOrderingSerializerSnapshot
  override def createInstance(): Ordering[Double] = Ordering.Double.TotalOrdering
}

class DefaultDoubleOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Double]](() => DefaultDoubleOrderingSerializer)

// Ordering.Double.IeeeOrdering

object DoubleIeeeOrderingSerializer extends ConstantOrderingSerializer[Double] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Double]] =
    new DoubleIeeeOrderingSerializerSnapshot
  override def createInstance(): Ordering[Double] = Ordering.Double.IeeeOrdering
}

class DoubleIeeeOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[Double]](() => DoubleIeeeOrderingSerializer)

// Ordering[BigInt]

object DefaultBigIntOrderingSerializer extends ConstantOrderingSerializer[BigInt] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[BigInt]] =
    new DefaultBigIntOrderingSerializerSnapshot
  override def createInstance(): Ordering[BigInt] = Ordering.BigInt
}

class DefaultBigIntOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[BigInt]](() => DefaultBigIntOrderingSerializer)

// Ordering[BigDecimal]

object DefaultBigDecimalOrderingSerializer extends ConstantOrderingSerializer[BigDecimal] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[BigDecimal]] =
    new DefaultBigDecimalOrderingSerializerSnapshot
  override def createInstance(): Ordering[BigDecimal] = Ordering.BigDecimal
}

class DefaultBigDecimalOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[BigDecimal]](() => DefaultBigDecimalOrderingSerializer)

// Ordering[String]

object DefaultStringOrderingSerializer extends ConstantOrderingSerializer[String] {
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[String]] =
    new DefaultStringOrderingSerializerSnapshot
  override def createInstance(): Ordering[String] = Ordering.String
}

class DefaultStringOrderingSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[Ordering[String]](() => DefaultStringOrderingSerializer)

// Reverse

class ReverseOrderingSerializer[A](child: TypeSerializer[Ordering[A]]) extends ImmutableSerializer[Ordering[A]] {
  override val isImmutableType: Boolean             = child.isImmutableType
  override def copy(from: Ordering[A]): Ordering[A] = {
    if (from == null || isImmutableType) {
      from
    } else {
      child.copy(from.reverse).reverse
    }
  }
  override def getLength: Int                                               = child.getLength
  override def serialize(record: Ordering[A], target: DataOutputView): Unit = child.serialize(record.reverse, target)
  override def deserialize(reuse: Ordering[A], source: DataInputView): Ordering[A] =
    child.deserialize(reuse.reverse, source).reverse
  override def deserialize(source: DataInputView): Ordering[A]              = child.deserialize(source).reverse
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[A]] =
    new ReverseOrderingSerializerSnapshot[A](child)
  override def createInstance(): Ordering[A] = child.createInstance().reverse
}

class ReverseOrderingSerializerSnapshot[A](private var child: TypeSerializer[Ordering[A]])
    extends TypeSerializerSnapshot[Ordering[A]] {
  def this() = this(null) // Empty constructor is required to instantiate this class during deserialization.
  override def getCurrentVersion: Int                   = 1
  override def writeSnapshot(out: DataOutputView): Unit =
    TypeSerializerSnapshot.writeVersionedSnapshot(out, child.snapshotConfiguration())
  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    child = TypeSerializerSnapshot.readVersionedSnapshot[Ordering[A]](in, userCodeClassLoader).restoreSerializer()
  }
  override def resolveSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[Ordering[A]]
  ): TypeSerializerSchemaCompatibility[Ordering[A]] =
    TypeSerializerSchemaCompatibility.compatibleAsIs()
  override def restoreSerializer(): TypeSerializer[Ordering[A]] = new ReverseOrderingSerializer[A](child)
}

// Option

class OptionOrderingSerializer[A](child: TypeSerializer[Ordering[A]]) extends ImmutableSerializer[Ordering[Option[A]]] {
  override val isImmutableType: Boolean                             = child.isImmutableType
  override def copy(from: Ordering[Option[A]]): Ordering[Option[A]] = {
    if (from == null || isImmutableType) {
      from
    } else {
      Ordering.Option(child.copy(from.asInstanceOf[OptionOrdering[A]].optionOrdering))
    }
  }
  override def getLength: Int                                                       = child.getLength
  override def serialize(record: Ordering[Option[A]], target: DataOutputView): Unit =
    child.serialize(record.asInstanceOf[OptionOrdering[A]].optionOrdering, target)
  override def deserialize(reuse: Ordering[Option[A]], source: DataInputView): Ordering[Option[A]] =
    Ordering.Option(child.deserialize(reuse.asInstanceOf[OptionOrdering[A]].optionOrdering, source))
  override def deserialize(source: DataInputView): Ordering[Option[A]] = Ordering.Option(child.deserialize(source))
  override def snapshotConfiguration(): TypeSerializerSnapshot[Ordering[Option[A]]] =
    new OptionOrderingSerializerSnapshot[A](child)
  override def createInstance(): Ordering[Option[A]] = Ordering.Option(child.createInstance())
}

class OptionOrderingSerializerSnapshot[A](private var child: TypeSerializer[Ordering[A]])
    extends TypeSerializerSnapshot[Ordering[Option[A]]] {
  def this() = this(null) // Empty constructor is required to instantiate this class during deserialization.
  override def getCurrentVersion: Int                   = 1
  override def writeSnapshot(out: DataOutputView): Unit =
    TypeSerializerSnapshot.writeVersionedSnapshot(out, child.snapshotConfiguration())
  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    child = TypeSerializerSnapshot.readVersionedSnapshot[Ordering[A]](in, userCodeClassLoader).restoreSerializer()
  }
  override def resolveSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[Ordering[Option[A]]]
  ): TypeSerializerSchemaCompatibility[Ordering[Option[A]]] =
    TypeSerializerSchemaCompatibility.compatibleAsIs()
  override def restoreSerializer(): TypeSerializer[Ordering[Option[A]]] = new OptionOrderingSerializer[A](child)
}
