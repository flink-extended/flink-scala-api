package org.apache.flinkx.api.rowdata

import scala.deriving.Mirror

/** Provides automatic derivation of [[RowDataConverter]] for every case class in scope.
  *
  * {{{
  * import org.apache.flinkx.api.rowdata.auto.given
  *
  * case class User(id: String, age: Int) // no `derives` clause needed
  *
  * val user = summon[RowDataConverter[User]].fromRowData(row)
  * }}}
  *
  * The convenience costs compile time: the converter is re-derived at every use site, including once per nested field
  * of every enclosing case class. Prefer [[semiauto]] or a `derives` clause for types used in more than a couple of
  * places.
  *
  * @see
  *   [[semiauto]] for explicit derivation
  */
trait auto {

  inline given autoRowDataConverter[T](using m: Mirror.ProductOf[T]): RowDataConverter[T] =
    RowDataConverter.derived[T]

}

object auto extends auto
