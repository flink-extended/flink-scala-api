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
package org.apache.flinkx.api.typeinfo

import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.serializer.{EitherSerializer, NothingSerializer}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

/** TypeInformation [[Either]].
  */
@Public
class EitherTypeInfo[A, B, T <: Either[A, B]](
    val clazz: Class[T],
    val leftTypeInfo: TypeInformation[A],
    val rightTypeInfo: TypeInformation[B]
) extends TypeInformation[T] {

  @PublicEvolving
  override def isBasicType: Boolean = false
  @PublicEvolving
  override def isTupleType: Boolean = false
  @PublicEvolving
  override def isKeyType: Boolean = false
  @PublicEvolving
  override def getTotalFields: Int = 1
  @PublicEvolving
  override def getArity: Int = 1
  @PublicEvolving
  override def getTypeClass: Class[T] = clazz

  @PublicEvolving
  @nowarn("cat=deprecation")
  override def getGenericParameters: java.util.Map[String, TypeInformation[_]] =
    Map[String, TypeInformation[_]]("A" -> leftTypeInfo, "B" -> rightTypeInfo).asJava

  override def createSerializer(config: SerializerConfig): TypeSerializer[T] = {
    val leftSerializer: TypeSerializer[A] = if (leftTypeInfo != null) {
      leftTypeInfo.createSerializer(config)
    } else {
      (new NothingSerializer).asInstanceOf[TypeSerializer[A]]
    }

    val rightSerializer: TypeSerializer[B] = if (rightTypeInfo != null) {
      rightTypeInfo.createSerializer(config)
    } else {
      (new NothingSerializer).asInstanceOf[TypeSerializer[B]]
    }
    new EitherSerializer[A, B](leftSerializer, rightSerializer).asInstanceOf[TypeSerializer[T]]
  }

  // override modifier removed to satisfy both implementation requirement of Flink 1.x and removal in 2.x
  def createSerializer(config: ExecutionConfig): TypeSerializer[T] = null

  override def equals(obj: Any): Boolean = {
    obj match {
      case eitherTypeInfo: EitherTypeInfo[_, _, _] =>
        eitherTypeInfo.canEqual(this) &&
        clazz.equals(eitherTypeInfo.clazz) &&
        leftTypeInfo.equals(eitherTypeInfo.leftTypeInfo) &&
        rightTypeInfo.equals(eitherTypeInfo.rightTypeInfo)
      case _ => false
    }
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[EitherTypeInfo[_, _, _]]
  }

  override def hashCode(): Int = {
    31 * (31 * clazz.hashCode() + leftTypeInfo.hashCode()) + rightTypeInfo.hashCode()
  }

  override def toString = s"Either[$leftTypeInfo, $rightTypeInfo]"
}
