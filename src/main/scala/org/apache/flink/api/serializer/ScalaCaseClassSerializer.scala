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

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}

import java.io.ObjectInputStream

/** This is a non macro-generated, concrete Scala case class serializer. Copied from Flink 1.14 with two changes:
  *   1. Does not extend `SelfResolvingTypeSerializer`, since we're breaking compatibility anyway. 2. Move
  *      `lookupConstructor` to version-specific sources.
  */
@SerialVersionUID(1L)
class ScalaCaseClassSerializer[T <: Product](
    clazz: Class[T],
    scalaFieldSerializers: Array[TypeSerializer[_]]
) extends CaseClassSerializer[T](clazz, scalaFieldSerializers)
    with ConstructorCompat {
  private[this] val numFields = scalaFieldSerializers.length

  // In Flink, serializers & serializer snapshotters have strict ser/de requirements.
  // Both need to be capable of creating one another.
  // Anything passed to a serializer therefore needs to be ser/de compatible.
  // The easiest method is to serialize class names during the snapshotting phase.
  // During restoration, those class names are deserialized and instantiated via a class loader.
  // Underlying implementation is major version-specific (Scala 2 vs. Scala 3).
  @transient
  private var constructor = lookupConstructor(clazz, numFields)

  override def createInstance(fields: Array[AnyRef]): T = {
    constructor(fields)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new ScalaCaseClassSerializerSnapshot[T](this)

  // Do NOT delete this method, it is used by ser/de even though it is private.
  // This should be removed once we make sure that serializer are no long java serialized.
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    constructor = lookupConstructor(clazz, numFields)
  }
}
