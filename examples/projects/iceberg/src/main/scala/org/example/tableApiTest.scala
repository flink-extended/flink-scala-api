package org.example

import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

import org.apache.flink.table.data.RowData
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.configuration.RestOptions.BIND_PORT
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation

import org.apache.hadoop.conf.Configuration as HadoopCfg

import org.apache.iceberg.flink.TableLoader
import org.apache.iceberg.flink.source.IcebergSource
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory

import scala.jdk.CollectionConverters.*

object tableApiTest:

  def main(args: Array[String]): Unit =
    val endpoint = args.headOption.getOrElse("http://minio:9000")
    println(s"endpoint: $endpoint")

    val config = Configuration.fromMap(
      Map(
        //ConfigConstants.LOCAL_START_WEBSERVER -> "true",
        BIND_PORT.key -> "8081",
        "execution.checkpointing.interval" -> "10 s"
      ).asJava
    )
    val env =
      StreamExecutionEnvironment.getExecutionEnvironment // .createLocalEnvironmentWithWebUI(config)
    val hadoopCfg = HadoopCfg()
    hadoopCfg.set("fs.s3a.access.key", "admin")
    hadoopCfg.set("fs.s3a.secret.key", "password")
    hadoopCfg.set("fs.s3a.endpoint", endpoint)
    hadoopCfg.set("fs.s3a.connection.ssl.enabled", "false")
    hadoopCfg.set("fs.s3a.path.style.access", "true")
    hadoopCfg.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopCfg.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    val tableLoader =
      TableLoader.fromHadoopTable(
        "s3a://vvp/icebergcatalog/default/sample",
        hadoopCfg
      )
    val source = IcebergSource
      .forRowData()
      .tableLoader(tableLoader)
      .assignerFactory(SimpleSplitAssignerFactory())
      .build()

    given rowDataType: TypeInformation[RowData] =
      TypeInformation.of(classOf[RowData])
    val batch = env.fromSource[RowData](
      source,
      WatermarkStrategy.noWatermarks(),
      "My Iceberg Source"
    )

    batch.map(r => s"${r.getLong(0)} | ${r.getString(1)}").print()

    env.execute("Test Iceberg Streaming Read")
