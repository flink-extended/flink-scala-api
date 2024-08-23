//> using dep "org.flinkextended::flink-scala-api:1.17.2_1.1.2"
//> using dep "org.apache.flink:flink-clients:1.17.1"
//> using dep "org.apache.flink:flink-csv:1.17.1"
//> using dep "org.apache.flink:flink-connector-files:1.17.1"
//> using dep "org.apache.flink:flink-connector-kafka:1.17.1"

import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.base.source.hybrid.HybridSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.core.fs.Path

val currentDirectory = java.io.File(".").getCanonicalPath

val fileSource = FileSource
  .forBulkFileFormat(
    StreamFormatAdapter(TextLineInputFormat()),
    Path(s"$currentDirectory/sink-table")
  )
  .build

val switchTimestamp = -1L
val brokers = "confluentkafka-cp-kafka:9092"

val kafkaSource = KafkaSource
  .builder[String]()
  .setBootstrapServers(brokers)
  .setTopics("bids")
  .setStartingOffsets(OffsetsInitializer.timestamp(switchTimestamp + 1))
  .setValueOnlyDeserializer(SimpleStringSchema())
  .build

val hybridSource = HybridSource
  .builder(fileSource)
  .addSource(kafkaSource)
  .build

val env = StreamExecutionEnvironment.getExecutionEnvironment
env
  .fromSource(hybridSource, WatermarkStrategy.noWatermarks(), "combined")
  .print()

env.execute()
