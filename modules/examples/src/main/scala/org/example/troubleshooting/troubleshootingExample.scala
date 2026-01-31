package org.example

import java.time.Duration

import org.apache.flinkx.api._
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.function.ProcessWindowFunction

import org.apache.commons.lang3.RandomStringUtils

import io.bullet.borer.{Codec, Decoder, Encoder, Cbor}

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.RestOptions.BIND_PORT
import org.apache.flink.configuration.TaskManagerOptions.{
  CPU_CORES,
  MANAGED_MEMORY_SIZE,
  TASK_HEAP_MEMORY,
  TASK_OFF_HEAP_MEMORY
}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram
import org.apache.flink.util.Collector
import org.apache.flink.configuration.MemorySize

import scala.util.{Success, Failure, Random, Using}
import scala.io.Source

import Measurement.given
import WindowedMeasurements.given

case class Measurement(
    sensorId: Int,
    value: Double,
    location: String,
    measurementInformation: String
)

object Measurement:
  given codec: Codec[Measurement] = Codec(
    Encoder.forProduct[Measurement],
    Decoder.forProduct[Measurement]
  )

  given measurementTypeInformation: TypeInformation[Measurement] =
    TypeInformation.of(classOf[Measurement])

case class WindowedMeasurements(
    windowStart: Long = 0,
    windowEnd: Long = 0,
    location: String = "",
    eventsPerWindow: Long = 0,
    sumPerWindow: Double = 0
):
  def addMeasurement(m: Measurement) =
    copy(
      sumPerWindow = sumPerWindow + m.value,
      eventsPerWindow = eventsPerWindow + 1
    )

object WindowedMeasurements:
  given wmTypeInformation: TypeInformation[WindowedMeasurements] =
    TypeInformation.of(classOf[WindowedMeasurements])

case class WindowedMeasurementsForArea(
    windowStart: Long = 0,
    windowEnd: Long = 0,
    area: String = "",
    locations: Array[String] = Array.empty,
    eventsPerWindow: Long = 0,
    sumPerWindow: Double = 0
)

object WindowedMeasurementsForArea:
  def getArea(location: String): String =
    if location.length > 0 then location.substring(0, 1) else ""

class MeasurementDeserializer extends RichFlatMapFunction[FakeKafkaRecord, Measurement]:

  lazy val numInvalidRecords =
    getRuntimeContext.getMetricGroup.counter("numInvalidRecords")

  override def flatMap(
      value: FakeKafkaRecord,
      out: Collector[Measurement]
  ): Unit =
    val m = Cbor.decode(value.value).to[Measurement].valueTry
    m match
      case Failure(_)     => numInvalidRecords.inc
      case Success(value) => out.collect(value)

val RANDOM_SEED         = 1
val NUM_OF_MEASUREMENTS = 100_000

def createSerializedMeasurements: Array[Array[Byte]] =
  val rand      = Random(RANDOM_SEED)
  val locations =
    Using.resource(Source.fromResource("cities.csv"))(_.getLines().toArray)

  (0 to NUM_OF_MEASUREMENTS).map { _ =>
    val m = Measurement(
      rand.nextInt(100),
      rand.nextDouble * 100,
      locations(rand.nextInt(locations.length)),
      RandomStringUtils.randomAlphabetic(30)
    )
    Cbor.encode(m).toByteArray
  }.toArray

@main def main =
  val flinkConfig = Configuration()
  flinkConfig.set(BIND_PORT, "8080")
  flinkConfig.set(CPU_CORES, 4.0)
  flinkConfig.set(TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(1024))
  flinkConfig.set(TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(256))
  flinkConfig.set(MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(1024))

  val env =
    StreamExecutionEnvironment
      // .createLocalEnvironmentWithWebUI(flinkConfig)
      .getExecutionEnvironment
      .setBufferTimeout(10)

  env.getConfig.setAutoWatermarkInterval(100)
  env.enableCheckpointing(5000)
  env.getConfig.disableForceKryo()
  env.getCheckpointConfig.setMinPauseBetweenCheckpoints(4000)

  val FAILURE_RATE = 0.0001f

  val sourceStream = env
    .addSource(
      FakeKafkaSource(
        RANDOM_SEED,
        Set(0, 4),
        createSerializedMeasurements,
        FAILURE_RATE
      )
    )
    .name("FakeKafkaSource")
    .uid("FakeKafkaSource")
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[FakeKafkaRecord](Duration.ofMillis(250))
        .withTimestampAssigner((rec, _) => rec.timestamp)
        .withIdleness(Duration.ofSeconds(1))
    )
    .name("Watermarks")
    .uid("Watermarks")
    .flatMap(MeasurementDeserializer())
    .name("Deserialization")
    .uid("Deserialization")

  val aggregatedPerLocation =
    sourceStream
      .keyBy(_.location)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .aggregate(
        MeasurementWindowAggregatingPerLocation(),
        MeasurementWindowProcessFunction()
      )
      .name("WindowedAggregationPerLocation")
      .uid("WindowedAggregationPerLocation")

  val aggregatedPerArea =
    aggregatedPerLocation
      .keyBy(m => WindowedMeasurementsForArea.getArea(m.location))
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .aggregate(MeasurementWindowAggregatingPerArea())

  aggregatedPerLocation
    .addSink(DiscardingSink())
    .name("OutputPerLocation")
    .uid("OutputPerLocation")
    .disableChaining

  aggregatedPerArea
    .print()
    .name("OutputPerArea")
    .uid("OutputPerArea")
    .disableChaining

  env.execute("troubleshootingExample.scala")
