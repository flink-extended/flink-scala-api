package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.deriving.Mirror
import scala.reflect.ClassTag

private[api] trait AutoImplicits extends TypeInformationDerivation:
  // Declare here implicit def that must have a lower priority than implicit def in Implicits

  /** Automatically derives TypeInformation for ADT (case classes, sealed traits) using a Scala 3's mirror-based
    * derivation and Magnolia.
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
    * val eventInfo: TypeInformation[Event] = summon[TypeInformation[Event]]
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
  final inline implicit def deriveTypeInformation[T](using
      m: Mirror.Of[T],
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): TypeInformation[T] = derived
