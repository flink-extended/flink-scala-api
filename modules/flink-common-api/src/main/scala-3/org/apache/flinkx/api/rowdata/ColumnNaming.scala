package org.apache.flinkx.api.rowdata

/** Names the column of a case class field in [[RowDataConverter.rowType]].
  *
  * The default keeps the field name. To match a table whose columns are `snake_case`, put the strategy in scope where
  * the converter is derived:
  *
  * {{{
  * given ColumnNaming = ColumnNaming.snakeCase
  *
  * case class Login(userId: String, lastSeenAt: LocalDate) derives RowDataConverter // user_id, last_seen_at
  * }}}
  *
  * Only the schema is affected: the converter maps columns by position either way.
  */
trait ColumnNaming extends Serializable {
  def columnName(fieldName: String): String
}

object ColumnNaming {

  given fieldName: ColumnNaming = name => name

  /** `userId` becomes `user_id`, `httpStatusCode` becomes `http_status_code`. */
  val snakeCase: ColumnNaming =
    _.replaceAll("([a-z0-9])([A-Z])", "$1_$2").replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").toLowerCase

}
