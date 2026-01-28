package org.example

import org.apache.flinkx.api.*
import org.apache.flinkx.api.auto.*

import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.triggers.Trigger.OnMergeContext
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.MapState

import scala.jdk.CollectionConverters.*

import java.util.concurrent.TimeUnit
import java.time.Duration

final case class TestEvent(
    key: Long,
    timestamp: Long,
    runningCount: Long,
    windowStart: Long = -1, // no window assigned yet,
    bag: List[Int] = Nil
)

class CustomEventTimeTrigger[T, W <: TimeWindow](trigger: EventTimeTrigger) extends Trigger[T, W]:

  override def onElement(
      element: T,
      timestamp: Long,
      window: W,
      ctx: TriggerContext
  ): TriggerResult =
    val result = trigger.onElement(element, timestamp, window, ctx)
    if result.isPurge then TriggerResult.FIRE_AND_PURGE
    else TriggerResult.FIRE

  override def onProcessingTime(
      time: Long,
      window: W,
      ctx: TriggerContext
  ): TriggerResult = trigger.onProcessingTime(time, window, ctx)

  override def onEventTime(
      time: Long,
      window: W,
      ctx: TriggerContext
  ): TriggerResult = trigger.onEventTime(time, window, ctx);

  override def clear(window: W, ctx: TriggerContext): Unit =
    trigger.clear(window, ctx)

  override def canMerge: Boolean = trigger.canMerge

  override def onMerge(window: W, ctx: OnMergeContext): Unit =
    trigger.onMerge(window, ctx)

  override def toString: String =
    s"CustomEventTimeTrigger(${trigger.toString})"

def windowActionJ(
    key: Long,
    window: TimeWindow,
    input: _root_.java.lang.Iterable[TestEvent],
    out: Collector[TestEvent]
): Unit = windowAction(key, window, input.asScala, out)

def windowAction(
    key: Long,
    window: TimeWindow,
    input: Iterable[TestEvent],
    out: Collector[TestEvent]
): Unit =
  val reduced = input.reduce(reduceEvents)
  val output  =
    reduced.copy(
      windowStart = window.getStart,
      runningCount = if reduced.runningCount > 0 then reduced.runningCount else 1
    )
  println(
    s"\n{start: ${window.getStart} .. end: ${window.getEnd}, count: ${output.runningCount} \ninput: "
  )
  println(input.mkString(" ", "\n", ""))

  out.collect(output)
  println(s"output: $output }")

def reduceEvents(a: TestEvent, b: TestEvent) =
  val (latest, prev) =
    if b.timestamp > a.timestamp then (b, a)
    else (a, b)
  val prevCount = if prev.runningCount == 0 then 1 else prev.runningCount
  latest.copy(runningCount = prevCount + 1)

/*
'runningWindowedSum' is using window function and custom trigger to emit running count on every element
 */
@main def runningWindowedSum =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val windowSize        = Time.of(10, TimeUnit.SECONDS)
  val windowSlide       = Time.of(2, TimeUnit.SECONDS)
  val watermarkStrategy = WatermarkStrategy
    .forBoundedOutOfOrderness[TestEvent](Duration.ofSeconds(1000))
    .withTimestampAssigner((event: TestEvent, streamRecordTimestamp: Long) => event.timestamp)

  env
    .fromElements(
      TestEvent(2L, 6000, 0),
      TestEvent(2L, 5000, 0),
      TestEvent(2L, 12000, 0)
    )
    .assignTimestampsAndWatermarks(watermarkStrategy)
    .keyBy(_.key)
    .window(SlidingEventTimeWindows.of(windowSize, windowSlide))
    .trigger(CustomEventTimeTrigger(EventTimeTrigger.create()))
    .reduce(reduceEvents, windowAction)

  env.execute()

class RunningCountFunc(windowSize: Duration) extends KeyedProcessFunction[Long, TestEvent, TestEvent]:

  val oldEntriesCleanupInterval                      = 1000L
  var minTimestamp: ValueState[Long]                 = _
  var timeToCount: MapState[Long, Long]              = _
  override def open(parameters: Configuration): Unit =
    timeToCount = getRuntimeContext.getMapState(
      MapStateDescriptor("timestamp2count", classOf[Long], classOf[Long])
    )

    minTimestamp = getRuntimeContext.getState(
      new ValueStateDescriptor(
        "min-timestamp",
        classOf[Long],
        Long.MinValue
      )
    )

  override def processElement(
      event: TestEvent,
      ctx: KeyedProcessFunction[Long, TestEvent, TestEvent]#Context,
      out: Collector[TestEvent]
  ): Unit =
    val currentCount =
      if timeToCount.contains(event.timestamp) then timeToCount.get(event.timestamp)
      else 0
    timeToCount.put(event.timestamp, currentCount + 1)

    val windowStart = event.timestamp - windowSize.getSeconds * 1000
    val windowCount =
      timeToCount.entries().asScala.foldLeft(0L) { (acc, entry) =>
        if (windowStart < entry.getKey) && (entry.getKey <= event.timestamp)
        then acc + entry.getValue
        else acc
      }

    minTimestamp.update(
      windowStart // TODO: substract some out of orderness threshold
    )
    out.collect(
      event.copy(runningCount = windowCount, windowStart = windowStart)
    )

    ctx.timerService.registerProcessingTimeTimer(
      System.currentTimeMillis + oldEntriesCleanupInterval
    )

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Long, TestEvent, TestEvent]#OnTimerContext,
      out: Collector[TestEvent]
  ): Unit =
    println(s"Clean up for ${minTimestamp.value()}")

    val oldEntries = timeToCount
      .entries()
      .asScala
      .collect {
        case entry if entry.getKey < minTimestamp.value() => entry.getKey
      }
    oldEntries.foreach(timeToCount.remove)

/*
'runningSum' example is using a ProcessFunction with state variables to report running count within a time window specfied
 */
@main def runningSum =
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env
    .fromElements(
      TestEvent(2L, 6000, 0),
      TestEvent(2L, 5000, 0),
      TestEvent(2L, 15000, 0)
    )
    .keyBy(_.key)
    .process(RunningCountFunc(Duration.ofSeconds(10)))
    .print()

  env.execute()
