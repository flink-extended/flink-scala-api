package org.example.rowdata

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.{IntType, RowType, VarCharType}
import org.apache.flinkx.api.*
import org.apache.flinkx.api.rowdata.*
import org.apache.flinkx.api.serializers.*

/** Converting a stream of Flink `RowData` into a Scala case class.
  *
  * `RowData` is Flink's internal row format: it is what table connectors and formats hand you, and unlike `Row` it has
  * no generic `getField` — only typed accessors like `getInt` and `getString`. `RowDataConverter` derives the right
  * sequence of those accessor calls at compile time.
  *
  * Fields map '''by position''': `User`'s fields must be declared in the same order as the table's columns. Nothing
  * checks this for you, so keep the case class and the `RowType` below side by side.
  */
case class User(id: String, name: String, age: Int) derives RowDataConverter

@main def basicRowDataExample =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // The schema of the incoming rows. In a real job this comes from the connector, not from you.
  val rowType = RowType.of(
    Array[org.apache.flink.table.types.logical.LogicalType](
      new VarCharType(VarCharType.MAX_LENGTH),
      new VarCharType(VarCharType.MAX_LENGTH),
      new IntType()
    ),
    Array("id", "name", "age")
  )

  // A DataStream of RowData needs the matching InternalTypeInfo to be serialized between operators.
  given TypeInformation[RowData] = InternalTypeInfo.of(rowType)

  val rows = env.fromElements[RowData](
    GenericRowData.of(StringData.fromString("u1"), StringData.fromString("Alice"), Integer.valueOf(30)),
    GenericRowData.of(StringData.fromString("u2"), StringData.fromString("Bob"), Integer.valueOf(25))
  )

  rows
    .map(_.toScala[User]) // RowData -> User
    .filter(_.age >= 30)
    .print()

  env.execute("basicRowData")
