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
import org.slf4j.{Logger, LoggerFactory}

import java.io.ObjectInputStream
import scala.util.{Failure, Success, Try}

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

  private val numFields = scalaFieldSerializers.length

  @transient lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  override val isImmutableType: Boolean = isCaseClassImmutable &&
    scalaFieldSerializers.forall(Option(_).exists(_.isImmutableType))
  val isImmutableSerializer: Boolean =
    scalaFieldSerializers.forall(Option(_).forall(s => s.duplicate().eq(s)))

  // In Flink, serializers & serializer snapshotters have strict ser/de requirements.
  // Both need to be capable of creating one another.
  // Anything passed to a serializer therefore needs to be ser/de compatible.
  // The easiest method is to serialize class names during the snapshotting phase.
  // During restoration, those class names are deserialized and instantiated via a class loader.
  // The underlying implementation is major version-specific (Scala 2 vs. Scala 3).
  @transient
  private var constructor = lookupConstructor(clazz, numFields)

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

  def serialize(value: T, target: DataOutputView): Unit = {
    // Null value is handled by setting arity field to -1
    val sourceArity = if (value == null) -1 else arity
    target.writeInt(sourceArity)

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
    var i           = 0
    var fieldFound  = true
    val sourceArity = source.readInt()
    if (sourceArity == -1) {
      null.asInstanceOf[T]
    } else {
      val fields = new Array[AnyRef](sourceArity)
      while (i < sourceArity && fieldFound) {
        Try(fieldSerializers(i).deserialize(source)) match {
          case Failure(e) =>
            log.warn(s"Failed to deserialize field at '$i' index", e)
            fieldFound = false
          case Success(value) =>
            fields(i) = value
        }
        i += 1
      }
      createInstance(fields.filter(_ != null))
    }
  }

  override def createInstance(fields: Array[AnyRef]): T = {
    constructor(fields)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new ScalaCaseClassSerializerSnapshot[T](this)

  // Do NOT delete this method, it is used by ser/de even though it is private.
  // This should be removed once we make sure that serializer is no longer java serialized.
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    constructor = lookupConstructor(clazz, numFields)
  }

}
