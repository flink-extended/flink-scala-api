package org.apache.flinkadt.api

import scala.quoted.*

// A basic replacement for `TypeTag`, which is absent in Scala 3.
trait TypeTag[A]:
  // Is the type a module, i.e. is it a case object?
  def isModule: Boolean

object TypeTag:
  def apply[A: TypeTag]: TypeTag[A] = summon

  // Fine to use `inline given` here, since usage is exclusive to Scala 3.
  inline given derived[A]: TypeTag[A] = ${ TypeTagMacro.gen[A] }
