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
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.NullFieldException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/** Serializer for Case Classes. Creation and access is different from our Java Tuples so we have to treat them
  * differently. Copied from Flink 1.14.
  */
@Internal
@SerialVersionUID(7341356073446263475L)
abstract class CaseClassSerializer[T <: Product](
    clazz: Class[T],
    scalaFieldSerializers: Array[TypeSerializer[_]]
) extends TupleSerializerBase[T](clazz, scalaFieldSerializers)
    with Cloneable {

  @transient lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def isImmutableType: Boolean =
    scalaFieldSerializers.forall(s => Option(s).exists(_.isImmutableType))

  override def duplicate: CaseClassSerializer[T] =
    clone().asInstanceOf[CaseClassSerializer[T]]

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
    if (from == null) {
      null.asInstanceOf[T]
    } else {
      val fields = (0 until arity).map(i => fieldSerializers(i).copy(from.productElement(i).asInstanceOf[AnyRef]))
      createInstance(fields.toArray)
    }

  private def isClassArityUsageDisabled =
    sys.env
      .get("DISABLE_CASE_CLASS_ARITY_USAGE")
      .map(v =>
        Try(v.toBoolean)
          .getOrElse(false)
      )
      .exists(_ == true)

  def serialize(value: T, target: DataOutputView): Unit = {
    if (arity > 0 && !isClassArityUsageDisabled)
      target.writeInt(value.productArity)

    (0 until arity).foreach { i =>
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
    deserializeFromSource(source, isClassArityUsageDisabled)

  def deserialize(source: DataInputView): T =
    deserializeFromSource(source, isClassArityUsageDisabled)

  private[api] def deserializeFromSource(source: DataInputView, classArityUsageDisabled: Boolean): T = {
    var i           = 0
    var fieldFound  = true
    val sourceArity = if (arity > 0 && !classArityUsageDisabled) Try(source.readInt()).getOrElse(arity) else arity
    val fields      = new Array[AnyRef](arity)
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
