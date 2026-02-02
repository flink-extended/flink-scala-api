package org.apache.flinkx.api

import magnolia1.Magnolia
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.language.experimental.macros

private[api] trait AutoDerivationImplicits extends TypeInformationDerivation {
  // Declare here implicit def that must have a lower priority than implicit def in Implicits

  /** Automatically derives TypeInformation for ADT (case classes, sealed traits) using a Magnolia macro.
    *
    * This method is implicit, so it will be automatically invoked by the compiler when a TypeInformation[T] is needed
    * and T is an ADT.
    *
    * ==Usage==
    *
    * This method is called implicitly when importing `org.apache.flinkx.api.auto._`:
    *
    * {{{
    * import org.apache.flinkx.api.auto._
    *
    * case class Event(id: String, timestamp: Long)
    *
    * // deriveTypeInformation is called implicitly for Event
    * val eventInfo: TypeInformation[Event] = implicitly[TypeInformation[Event]]
    * }}}
    *
    * @tparam T
    *   the type to derive TypeInformation for
    * @return
    *   the derived TypeInformation[T]
    * @see
    *   [[semiauto.deriveTypeInformation]] for explicit derivation
    */
  // Must have a lower priority than more specific type-infos
  implicit def deriveTypeInformation[T]: TypeInformation[T] = macro Magnolia.gen[T]

}
