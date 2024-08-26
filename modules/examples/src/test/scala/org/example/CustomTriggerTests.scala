package org.example

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import org.scalatest.time.Millis
import org.scalatest.Inspectors

import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters.*

class CustomTriggerTest extends AnyFlatSpec with Matchers with Inspectors:

  it should "test custom trigger" in {
    val cfg = ExecutionConfig()
    val serializer = deriveTypeInformation[TestEvent].createSerializer(
      cfg
    )

    val stateDesc =
      ReducingStateDescriptor(
        "window-contents",
        reduceEvents,
        serializer
      )

    val windowSize = Time.of(10, TimeUnit.SECONDS)
    val windowSlide = Time.of(2, TimeUnit.SECONDS)

    val operator = WindowOperator(
      SlidingEventTimeWindows.of(windowSize, windowSlide),
      TimeWindow.Serializer(),
      (e: TestEvent) => e.key,
      TypeInformation.of(classOf[Long]).createSerializer(cfg),
      stateDesc,
      InternalSingleValueWindowFunction(windowActionJ _),
      CustomEventTimeTrigger(EventTimeTrigger.create()),
      0,
      null /* late data output tag */
    )

    val testHarness =
      KeyedOneInputStreamOperatorTestHarness[Long, TestEvent, TestEvent](
        operator,
        e => e.key,
        TypeInformation.of(classOf[Long])
      )

    testHarness.setup(serializer)
    testHarness.open()

    // closing all empty pre-generated windows
    testHarness.processWatermark(
      Watermark(
        windowSize.toMilliseconds - windowSlide.toMilliseconds
      )
    )
    testHarness.getOutput.size should be(1)
    testHarness.getRecordOutput.size should be(0)

    testHarness.processElement(
      StreamRecord(TestEvent(2L, 6000, 1, windowStart = -1, bag = List(3)), 6000)
    )
    testHarness.processElement(
      StreamRecord(TestEvent(2L, 5000, 0), 5000)
    )
    testHarness.processElement(
      StreamRecord(TestEvent(2L, 10000, 2), 10000)
    )

    println("content:")
    testHarness.getOutput.asScala.foreach(println)

    val records = testHarness.getRecordOutput.asScala
    records.size should be(12)
    val windows = records.groupBy(_.getValue.windowStart)

    // checking [0 - 10000] window, which contains 2 as running count
    windows(0)
      .map(_.getValue.runningCount)
      .max should be(2)

    // checking windows [2000 - 12000], [4000 - ..] etc., which contains 3 as running count
    windows
      .filterKeys(_ > 0)
      .mapValues(_.map(_.getValue.runningCount).max)
      .values
      .max should be(3)
  }
