package org.example

import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration
import org.apache.flinkx.api.function.ProcessWindowFunction
import org.apache.flink.api.common.functions.AggregateFunction

class MeasurementWindowProcessFunction
    extends ProcessWindowFunction[
      WindowedMeasurements,
      WindowedMeasurements,
      String,
      TimeWindow
    ]:
  val EVENT_TIME_LAG_WINDOW_SIZE = 10_000

  @transient lazy val eventTimeLag = getRuntimeContext.getMetricGroup
    .histogram(
      "eventTimeLag",
      DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE)
    )

  override def process(
      key: String,
      context: Context,
      elements: Iterable[WindowedMeasurements],
      out: Collector[WindowedMeasurements]
  ): Unit =
    val aggregate = elements.iterator.next
    val window = context.window
    val res = aggregate.copy(
      windowStart = window.getStart,
      windowEnd = window.getEnd,
      location = key
    )

    eventTimeLag.update(System.currentTimeMillis - window.getEnd)
    out.collect(res)

class MeasurementWindowAggregatingPerLocation
    extends AggregateFunction[
      Measurement,
      WindowedMeasurements,
      WindowedMeasurements
    ]:

  override def add(
      record: Measurement,
      accumulator: WindowedMeasurements
  ): WindowedMeasurements =
    accumulator.addMeasurement(record)

  override def createAccumulator(): WindowedMeasurements =
    WindowedMeasurements()

  override def getResult(
      accumulator: WindowedMeasurements
  ): WindowedMeasurements =
    accumulator

  override def merge(
      a: WindowedMeasurements,
      b: WindowedMeasurements
  ): WindowedMeasurements =
    b.copy(
      eventsPerWindow = a.eventsPerWindow + b.eventsPerWindow,
      sumPerWindow = a.sumPerWindow + b.sumPerWindow
    )

class MeasurementWindowAggregatingPerArea
    extends AggregateFunction[
      WindowedMeasurements,
      WindowedMeasurementsForArea,
      WindowedMeasurementsForArea
    ]:

  override def createAccumulator() = WindowedMeasurementsForArea()

  override def add(
      value: WindowedMeasurements,
      accumulator: WindowedMeasurementsForArea
  ): WindowedMeasurementsForArea =
    accumulator.copy(
      sumPerWindow = accumulator.sumPerWindow + value.sumPerWindow,
      eventsPerWindow = accumulator.eventsPerWindow + value.eventsPerWindow,
      locations = accumulator.locations :+ value.location
    )

  override def getResult(
      accumulator: WindowedMeasurementsForArea
  ): WindowedMeasurementsForArea =
    val l = accumulator.locations(0)
    accumulator.copy(area = WindowedMeasurementsForArea.getArea(l))

  override def merge(
      a: WindowedMeasurementsForArea,
      b: WindowedMeasurementsForArea
  ): WindowedMeasurementsForArea =
    b.copy(
      eventsPerWindow = a.eventsPerWindow + b.eventsPerWindow,
      sumPerWindow = a.sumPerWindow + b.sumPerWindow,
      locations = a.locations ++ b.locations
    )
