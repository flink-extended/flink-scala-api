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
package org.apache.flinkadt.api.typeinfo

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.core.memory.{DataInputView, DataOutputView, MemorySegment}

/** Comparator for [[Option]] values. Note that [[None]] is lesser than any [[Some]] values.
  */
@Internal
class OptionTypeComparator[A](ascending: Boolean, typeComparator: TypeComparator[A]) extends TypeComparator[Option[A]] {
  private var reference: Option[A] = _

  override def hash(record: Option[A]): Int = record.hashCode()

  override def compare(first: Option[A], second: Option[A]): Int = {
    first match {
      case Some(firstValue) =>
        second match {
          case Some(secondValue) => typeComparator.compare(firstValue, secondValue)
          case None =>
            if (ascending) {
              1
            } else {
              -1
            }
        }
      case None =>
        second match {
          case Some(_) =>
            if (ascending) {
              -1
            } else {
              1
            }
          case None => 0
        }
    }
  }

  override def compareSerialized(firstSource: DataInputView, secondSource: DataInputView): Int = {
    val firstSome  = firstSource.readBoolean()
    val secondSome = secondSource.readBoolean()

    if (firstSome) {
      if (secondSome) {
        typeComparator.compareSerialized(firstSource, secondSource)
      } else {
        if (ascending) {
          1
        } else {
          -1
        }
      }
    } else {
      if (secondSome) {
        if (ascending) {
          -1
        } else {
          1
        }
      } else {
        0
      }
    }
  }

  override def extractKeys(record: AnyRef, target: Array[AnyRef], index: Int): Int = {
    target(index) = record
    1
  }

  override def setReference(toCompare: Option[A]): Unit = {
    reference = toCompare
  }

  override def equalToReference(candidate: Option[A]): Boolean = {
    compare(reference, candidate) == 0
  }

  override def compareToReference(referencedComparator: TypeComparator[Option[A]]): Int = {
    compare(referencedComparator.asInstanceOf[this.type].reference, reference)
  }

  override lazy val getFlatComparators: Array[TypeComparator[_]] = {
    Array(this).asInstanceOf[Array[TypeComparator[_]]]
  }

  override def getNormalizeKeyLen: Int = 1 + typeComparator.getNormalizeKeyLen

  override def putNormalizedKey(
      record: Option[A],
      target: MemorySegment,
      offset: Int,
      numBytes: Int
  ): Unit = {
    if (numBytes >= 1) {
      record match {
        case Some(v) =>
          target.put(offset, OptionTypeComparator.OneInByte)
          typeComparator.putNormalizedKey(v, target, offset + 1, numBytes - 1)
        case None =>
          target.put(offset, OptionTypeComparator.ZeroInByte)
          var i = 1
          while (i < numBytes) {
            target.put(offset + i, OptionTypeComparator.ZeroInByte)
            i += 1
          }
      }
    }
  }

  override def invertNormalizedKey(): Boolean = !ascending

  override def readWithKeyDenormalization(reuse: Option[A], source: DataInputView): Option[A] = {
    throw new UnsupportedOperationException
  }

  override def writeWithKeyNormalization(record: Option[A], target: DataOutputView): Unit = {
    throw new UnsupportedOperationException
  }

  override def isNormalizedKeyPrefixOnly(keyBytes: Int): Boolean = {
    typeComparator.isNormalizedKeyPrefixOnly(keyBytes - 1)
  }

  override def supportsSerializationWithKeyNormalization() = false

  override def supportsNormalizedKey(): Boolean = typeComparator.supportsNormalizedKey()

  override def duplicate() = new OptionTypeComparator[A](ascending, typeComparator)
}

object OptionTypeComparator {
  val ZeroInByte: Byte = 0.asInstanceOf[Byte]
  val OneInByte: Byte  = 1.asInstanceOf[Byte]
}
