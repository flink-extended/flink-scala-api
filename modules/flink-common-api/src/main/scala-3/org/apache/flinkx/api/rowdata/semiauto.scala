package org.apache.flinkx.api.rowdata

import scala.deriving.Mirror

/** Provides semi-automatic (explicit) derivation of [[RowDataConverter]].
  *
  * Each case class states its converter once, in its companion object, so the derivation runs once per type rather than
  * once per use site:
  *
  * {{{
  * import org.apache.flinkx.api.rowdata.semiauto.*
  *
  * case class User(id: String, age: Int)
  *
  * object User:
  *   given RowDataConverter[User] = deriveRowDataConverter[User]
  * }}}
  *
  * The `derives RowDataConverter` clause on the case class is equivalent and shorter; this object exists for the cases
  * where the type cannot carry a `derives` clause, such as a class defined elsewhere.
  *
  * @see
  *   [[auto]] for automatic derivation
  */
trait semiauto {

  /** Explicitly derives a [[RowDataConverter]] for the case class `T`. */
  final inline def deriveRowDataConverter[T](using m: Mirror.ProductOf[T]): RowDataConverter[T] =
    RowDataConverter.derived[T]

}

object semiauto extends semiauto
