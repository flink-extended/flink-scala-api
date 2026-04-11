package org.apache.flinkx.api

import scala.quoted.*
import scala.reflect.ClassTag

// A basic replacement for `TypeTag`, which is absent in Scala 3.
trait TypeTag[A]:
  /** Is the type a module, i.e. is it a case object? */
  def isModule: Boolean
  /** Determines if this type can be cached, i.e. must not be generic */
  def isCachable: Boolean
  /** Is the type a Scala 3 enum? An enum value with parameters is not considered as enum to be handled as case class */
  def isEnum: Boolean
  /** Returns the companion object ClassTag of this type if it exists */
  def companion: Option[ClassTag[?]]
  def toString: String

object TypeTag:
  def apply[A: TypeTag]: TypeTag[A] = summon

  // Fine to use `inline given` here, since usage is exclusive to Scala 3.
  inline given derived[A]: TypeTag[A] = ${ TypeTagMacro.gen[A] }
