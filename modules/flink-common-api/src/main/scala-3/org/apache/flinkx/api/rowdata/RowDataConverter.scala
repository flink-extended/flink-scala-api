package org.apache.flinkx.api.rowdata

import org.apache.flink.table.data.{GenericRowData, RowData}

import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror

/** Converts between Flink [[RowData]] records and Scala case classes.
  *
  * Fields are mapped '''by position''': column `i` of the [[RowData]] corresponds to the `i`-th field in the case
  * class's declaration order. Nothing checks this against the table schema, so a case class whose fields are declared
  * in a different order than the table's columns will read garbage rather than fail.
  *
  * ==Usage==
  *
  * {{{
  * import org.apache.flinkx.api.rowdata.RowDataConverter
  *
  * case class User(id: String, name: String, age: Int) derives RowDataConverter
  *
  * val user = summon[RowDataConverter[User]].fromRowData(row)
  * }}}
  *
  * Per-field behaviour is customised by providing a `given` [[FieldConverter]] for the field's type; see
  * [[FieldConverter]].
  *
  * @see
  *   [[semiauto]] for explicit derivation, [[auto]] for automatic derivation of every case class in scope
  */
trait RowDataConverter[T] extends Serializable {

  /** Converts a [[RowData]] record into a `T`. */
  def fromRowData(row: RowData): T

  /** Converts a `T` into a [[RowData]] record, as a [[GenericRowData]] with row kind `INSERT`. */
  def toRowData(value: T): RowData

  /** Converts a `T` into a [[RowData]] record carrying the given [[RowKind]].
    *
    * Use this to preserve the changelog kind of record read from a table connector:
    * `value.toRowData(sourceRow.getRowKind)`.
    */
  def toRowData(value: T, rowKind: RowKind): RowData

  /** The number of columns `T` occupies. Required to read `T` back out of a nested `ROW` column. */
  def arity: Int

}

object RowDataConverter {

  /** Derives a converter for a case class, resolving one [[FieldConverter]] per field at compile time.
    *
    * Supports the `derives RowDataConverter` clause.
    */
  inline def derived[T](using m: Mirror.ProductOf[T]): RowDataConverter[T] =
    new DerivedRowDataConverter[T](m, summonConverters[m.MirroredElemTypes].toArray)

  private inline def summonConverters[Elems <: Tuple]: List[FieldConverter[?]] =
    inline erasedValue[Elems] match {
      case _: EmptyTuple     => Nil
      case _: (head *: tail) => summonInline[FieldConverter[head]] :: summonConverters[tail]
    }

  /** The generated converter. Public only because [[derived]] is `inline` and so constructs it at the call site; it is
    * an implementation detail and not part of the API.
    *
    * Field access goes through the pre-resolved `converters` array rather than through code unrolled per field. That
    * keeps the derivation a plain `inline` definition instead of a quoted macro, at the cost of an array load and a
    * virtual call per field — both of which the JIT resolves well, since the array is effectively final and each
    * position holds one monomorphic converter type.
    */
  final class DerivedRowDataConverter[T](
      mirror: Mirror.ProductOf[T],
      converters: Array[FieldConverter[?]]
  ) extends RowDataConverter[T] {

    val arity: Int = converters.length

    def fromRowData(row: RowData): T = {
      val fields = new Array[Any](arity)
      var i      = 0
      while (i < arity) {
        fields(i) = converters(i).fromRowData(row, i)
        i += 1
      }
      mirror.fromProduct(ArrayProduct(fields))
    }

    def toRowData(value: T): RowData = {
      val row     = new GenericRowData(arity)
      val product = value.asInstanceOf[Product]
      var i       = 0
      while (i < arity) {
        val converter = converters(i).asInstanceOf[FieldConverter[Any]]
        row.setField(i, converter.toRowData(product.productElement(i)))
        i += 1
      }
      row
    }

  }

  /** Adapts the decoded field array to the [[Product]] that [[Mirror.ProductOf.fromProduct]] consumes, without building
    * an intermediate `Tuple`.
    */
  private final class ArrayProduct(fields: Array[Any]) extends Product {
    def productArity: Int            = fields.length
    def productElement(n: Int): Any  = fields(n)
    def canEqual(that: Any): Boolean = that.isInstanceOf[ArrayProduct]
  }

}
