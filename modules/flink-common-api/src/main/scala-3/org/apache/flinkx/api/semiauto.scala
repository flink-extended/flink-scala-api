package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.deriving.Mirror
import scala.reflect.ClassTag

/** Provides semi-automatic (explicit) derivation of TypeInformation for Scala types.
  *
  * Import this object to have access to implicitly available type-information for Scala types, collections, Java types,
  * etc.
  *
  * Unlike [[auto]], this object requires explicit calls to `deriveTypeInformation` for ADT (case classes, sealed
  * traits). This gives you more control over where type-information is derived.
  *
  * ===Benefits of Explicit Derivation===
  *
  *  - '''Control''': Choose exactly which types have TypeInformation
  *  - '''Better compile times''': TypeInformation is derived once and cached
  *
  * ==Tuples==
  *
  * Note that tuples (Tuple2, Tuple3, Tuple4) are automatically derived even in `semiauto` for convenience. If you need
  * explicit control over tuples too, derive them manually.
  *
  * @see
  *   [[auto]] for automatic derivation
  * @see
  *   [[deriveTypeInformation]] for the explicit derivation method
  */
// Implicits priority order (linearization): semiauto > HighPrioImplicits
// Scala 3 doesn't allow to override implicit def so we cannot extend LowPrioImplicits
object semiauto extends TypeInformationDerivation with HighPrioImplicits {

  /** Explicitly derives TypeInformation for the given type T.
    *
    * This method must be called explicitly to derive TypeInformation for ADT (case classes and sealed traits).
    *
    * ===Usage===
    *
    * {{{
    * import org.apache.flinkx.api.semiauto._
    *
    * case class User(id: String, age: Int)
    *
    * object User {
    *   // Explicitly derive and cache TypeInformation
    *   implicit val userInfo: TypeInformation[User] = deriveTypeInformation[User]
    * }
    * }}}
    *
    * A good practice is to declare the type-information as implicit val in the companion object of the case class, it
    * is derived once, cached and will be available in the implicit context wherever the case class is used.
    *
    * @tparam T
    *   the type to derive TypeInformation for (must be an ADT)
    * @return
    *   the derived TypeInformation[T]
    */
  final inline def deriveTypeInformation[T](implicit
      m: Mirror.Of[T],
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): TypeInformation[T] = derived

  // Allow auto derivation on common tuples

  implicit def tuple2Info[
      A: TypeTag: TypeInformation,
      B: TypeTag: TypeInformation
  ]: TypeInformation[(A, B)] = deriveTypeInformation

  implicit def tuple3Info[
      A: TypeTag: TypeInformation,
      B: TypeTag: TypeInformation,
      C: TypeTag: TypeInformation
  ]: TypeInformation[(A, B, C)] = deriveTypeInformation

  implicit def tuple4Info[
      A: TypeTag: TypeInformation,
      B: TypeTag: TypeInformation,
      C: TypeTag: TypeInformation,
      D: TypeTag: TypeInformation
  ]: TypeInformation[(A, B, C, D)] = deriveTypeInformation

}
