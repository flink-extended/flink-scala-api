import $cp.lib.`flink-faker-0.4.0.jar`

import $ivy.`org.flinkextended::flink-scala-api:1.15.4_1.0.0`
import $ivy.`org.apache.flink:flink-clients:1.15.2`
import $ivy.`org.apache.flink:flink-csv:1.15.2`
import $ivy.`org.apache.flink:flink-table-api-java:1.15.2`
import $ivy.`org.apache.flink:flink-table-api-java-bridge:1.15.2`
import $ivy.`org.apache.flink:flink-table-runtime:1.15.2`
import $ivy.`org.apache.flink:flink-table-planner-loader:1.15.2`

import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions

import org.apache.flink.api._
import org.apache.flink.api.serializers._

import _root_.java.lang.{Long => JLong}

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env.getJavaEnv)
val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
val table = TableEnvironment.create(settings)


// table.createTemporaryTable(
//   "SourceTable",
//   TableDescriptor
//     .forConnector("datagen")
//     .schema(
//       Schema.newBuilder
//         .column("BookId", DataTypes.INT())
//         .build
//     )
//     .option(DataGenConnectorOptions.ROWS_PER_SECOND, new JLong(1))
//     .build
// )

val tableDescriptor = TableDescriptor
  .forConnector("faker")
  .schema(
    Schema.newBuilder
      .column(
        "id",
        DataTypes.INT // .notNull
      )
      .column(
        "a",
        DataTypes.ROW(DataTypes.FIELD("np", DataTypes.INT))
      )
      .build
  )
  .option("fields.id.expression", "#{number.numberBetween '0','10'}")
  .option("fields.a.np.expression", "#{number.numberBetween '20','30'}")
  // .option("fields.a.np.null-rate", "0.5")
  .option("fields.a.null-rate", "0.5")
  .option("rows-per-second", "50")
  .build
table.createTemporaryTable("t1", tableDescriptor)
// table.dropTemporaryTable("t1")

val res = table.executeSql(
  "SELECT a.id, COALESCE(a.a.np, a.id) c1, IFNULL(a.a.np, a.id) c2, a.a.np FROM t1 a"
  // "show create table t1"
)
res.print
