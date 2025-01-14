package org.example.fraud

import org.example.Transaction
import org.example.Alert

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.OutputTag
import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext
import org.apache.flink.streaming.api.operators.StreamFlatMap
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer
import org.example.fraud.FraudDetector

class FraudDetectorTest extends AnyFlatSpec with Matchers:

  class FakeCollector extends Collector[Alert]:
    val state = ListBuffer.empty[Alert]

    override def collect(record: Alert): Unit =
      state += record
    override def close(): Unit =
      state.clear

  def makeContext(detector: FraudDetector) =
    new detector.Context:
      override def getCurrentKey(): Long                              = ???
      override def output[X](outputTag: OutputTag[X], value: X): Unit = ???
      override def timestamp(): java.lang.Long                        = 0L
      override def timerService(): TimerService = new TimerService:
        override def currentProcessingTime(): Long                 = 0L
        override def registerProcessingTimeTimer(time: Long): Unit = ()
        override def currentWatermark(): Long                      = ???
        override def deleteProcessingTimeTimer(time: Long): Unit   = ()
        override def registerEventTimeTimer(time: Long): Unit      = ???
        override def deleteEventTimeTimer(time: Long): Unit        = ???

  it should "detect fraud" in {
    // given
    val detector = FraudDetector()
    detector.setRuntimeContext(FakeRuntimeContext())
    detector.open(new Configuration())

    val ctx       = makeContext(detector)
    val collector = FakeCollector()
    // when
    detector.processElement(Transaction(1, 1, 0.1), ctx, collector)
    // then
    collector.state should be(empty)
    // when
    detector.processElement(Transaction(1, 1, 500.1), ctx, collector)
    // then
    collector.state shouldNot be(empty)
  }
