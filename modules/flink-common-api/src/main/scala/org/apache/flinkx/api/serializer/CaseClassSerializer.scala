/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flinkx.api.serializer

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.NullFieldException
import org.apache.flinkx.api.serializer.CaseClassSerializer.EmptyByteArray
import org.slf4j.{Logger, LoggerFactory}

/** Serializer for Case Classes. Creation and access is different from our Java Tuples so we have to treat them
  * differently. Copied from Flink 1.14 and merged with ScalaCaseClassSerializer.
  */
@Internal
@SerialVersionUID(7341356073446263475L)
class CaseClassSerializer[T <: Product](
    clazz: Class[T],
    scalaFieldSerializers: Array[TypeSerializer[_]],
    val isCaseClassImmutable: Boolean
) extends TupleSerializerBase[T](clazz, scalaFieldSerializers)
    with Cloneable
    with ConstructorCompat {

  @transient private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  private val nullPadding: Array[Byte] = if (super.getLength > 0) new Array(super.getLength) else EmptyByteArray

  override val isImmutableType: Boolean = isCaseClassImmutable && fieldSerializers.forall(_.isImmutableType)
  val isImmutableSerializer: Boolean    = fieldSerializers.forall(s => s.duplicate().eq(s))

  // In Flink, serializers & serializer snapshotters have strict ser/de requirements.
  // Both need to be capable of creating one another.
  // Anything passed to a serializer therefore needs to be ser/de compatible.
  // The easiest method is to serialize class names during the snapshotting phase.
  // During restoration, those class names are deserialized and instantiated via a class loader.
  // The underlying implementation is major version-specific (Scala 2 vs. Scala 3).
  @transient private lazy val constructor = lookupConstructor(tupleClass)

  override def duplicate(): CaseClassSerializer[T] = {
    if (isImmutableSerializer) {
      this
    } else {
      clone().asInstanceOf[CaseClassSerializer[T]]
    }
  }

  @throws[CloneNotSupportedException]
  override protected def clone(): Object = {
    val result = super.clone().asInstanceOf[CaseClassSerializer[T]]
    // achieve a deep copy by duplicating the field serializers
    result.fieldSerializers = result.fieldSerializers.map(_.duplicate())
    result
  }

  def createInstance: T =
    try {
      val fields = (0 until arity).map(i => fieldSerializers(i).createInstance())
      createInstance(fields.toArray)
    } catch {
      case t: Throwable =>
        log.warn(s"Failed to create an instance returning null", t)
        null.asInstanceOf[T]
    }

  override def createOrReuseInstance(fields: Array[Object], reuse: T): T =
    createInstance(fields)

  def copy(from: T, reuse: T): T =
    copy(from)

  def copy(from: T): T =
    if (from == null || isImmutableType) {
      from
    } else {
      val fields = (0 until arity).map(i => fieldSerializers(i).copy(from.productElement(i).asInstanceOf[AnyRef]))
      createInstance(fields.toArray)
    }

  override val getLength: Int = if (super.getLength == -1) -1 else super.getLength + 4 // +4 bytes for the arity field

  def serialize(value: T, target: DataOutputView): Unit = {
    // Write an arity of -1 to indicate null value
    val sourceArity = if (value == null) -1 else arity
    target.writeInt(sourceArity)
    if (value == null) target.write(nullPadding)

    (0 until sourceArity).foreach { i =>
      val serializer = fieldSerializers(i).asInstanceOf[TypeSerializer[Any]]
      val o          = value.productElement(i)
      try serializer.serialize(o, target)
      catch {
        case e: NullPointerException =>
          throw new NullFieldException(i, e)
      }
    }
  }

  def deserialize(reuse: T, source: DataInputView): T =
    deserialize(source)

  def deserialize(source: DataInputView): T = {
    val sourceArity = source.readInt()
    if (sourceArity == -1) {
      source.skipBytesToRead(nullPadding.length)
      null.asInstanceOf[T]
    } else {
      val fields = new Array[AnyRef](sourceArity)
      var i      = 0
      while (i < sourceArity) {
        fields(i) = fieldSerializers(i).deserialize(source)
        i += 1
      }
      createInstance(fields)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val sourceArity = source.readInt()
    target.writeInt(sourceArity)
    if (sourceArity == -1) {
      source.skipBytesToRead(nullPadding.length)
      target.skipBytesToWrite(nullPadding.length)
    } else {
      super.copy(source, target)
    }
  }

  override def createInstance(fields: Array[AnyRef]): T = {
    constructor(fields)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new ScalaCaseClassSerializerSnapshot[T](this)

}

object CaseClassSerializer {
  private val EmptyByteArray: Array[Byte] = new Array(0)
}
