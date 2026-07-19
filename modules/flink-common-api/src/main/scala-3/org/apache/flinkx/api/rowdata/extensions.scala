package org.apache.flinkx.api.rowdata

import org.apache.flink.table.data.RowData

/** Syntax for converting between [[RowData]] and case classes without naming the converter.
  *
  * {{{
  * import org.apache.flinkx.api.rowdata.*
  *
  * case class User(id: String, age: Int) derives RowDataConverter
  *
  * val user: User    = row.toScala[User]
  * val back: RowData = user.toRowData
  * }}}
  */
extension (row: RowData) {

  /** Converts this record to `T`. The target type has to be written out: `row.toScala[User]`. */
  def toScala[T](using converter: RowDataConverter[T]): T = converter.fromRowData(row)

}

extension [T](value: T) {

  /** Converts this case class instance to a [[RowData]] record. */
  def toRowData(using converter: RowDataConverter[T]): RowData = converter.toRowData(value)

}
