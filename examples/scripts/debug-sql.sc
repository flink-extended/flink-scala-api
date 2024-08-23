import $ivy.`io.findify::flink-scala-api:1.15-2`

import $ivy.`org.apache.flink:flink-clients_2.12:1.14.6`

// import $ivy.`org.apache.flink:flink-streaming-java_2.12:1.14.6`
import $ivy.`org.apache.flink:flink-streaming-scala_2.12:1.14.6`

import $ivy.`org.apache.flink:flink-table-api-java:1.14.6`
import $ivy.`org.apache.flink:flink-table-api-java-bridge_2.12:1.14.6`
import $ivy.`org.apache.flink:flink-table-runtime_2.12:1.14.6`
import $ivy.`org.apache.flink:flink-table-planner_2.12:1.14.6`

import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions

import io.findify.flink.api._
import io.findify.flinkadt.api._

import java.lang.{Long => JLong}

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env.getJavaEnv)

val settings = EnvironmentSettings.newInstance().inStreamingMode().build()

val table = TableEnvironment.create(settings)

table.createTemporaryTable(
  "SourceTable",
  TableDescriptor
    .forConnector("datagen")
    .schema(
      Schema.newBuilder
        .column("BookId", DataTypes.INT())
        .build
    )
    .option(DataGenConnectorOptions.ROWS_PER_SECOND, new JLong(1))
    .build
)

val tableDescriptor = TableDescriptor
      .forConnector("datagen")
      .schema(
        Schema.newBuilder
          .column("id", DataTypes.INT.notNull)
          .column("a", DataTypes.ROW(DataTypes.FIELD("np", DataTypes.INT.notNull())).notNull())
          .build)
      .build
table.createTemporaryTable("t1", tableDescriptor)
table.createTemporaryTable("t2", tableDescriptor)
// table.dropTemporaryTable("t1")
// table.dropTemporaryTable("t2")

val res = table.executeSql("EXPLAIN SELECT a.id, COALESCE(a.a.np, b.a.np) c1, IFNULL(a.a.np, b.a.np) c2 FROM t1 a left JOIN t2 b ON a.id=b.id where a.a is null or a.a.np is null")
res.print
