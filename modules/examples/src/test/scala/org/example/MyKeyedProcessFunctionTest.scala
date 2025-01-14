package org.example

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector

@SerialVersionUID(1L)
class MyKeyedProcessFunction extends KeyedProcessFunction[Long, TestEvent, Long]:

  @throws[Exception]
  override def processElement(
      e: TestEvent,
      context: KeyedProcessFunction[Long, TestEvent, Long]#Context,
      out: Collector[Long]
  ): Unit =
    out.collect(e.timestamp)

class MyKeyedProcessFunctionTest extends AnyFlatSpec with Matchers:

  it should "test state" in {
    val operator =
      KeyedProcessOperator(MyKeyedProcessFunction())
    val testHarness =
      KeyedOneInputStreamOperatorTestHarness[Long, TestEvent, Long](
        operator,
        e => e.key,
        TypeInformation.of(classOf[Long])
      )

    testHarness.getExecutionConfig().setAutoWatermarkInterval(50)
    testHarness.setup()
    testHarness.open()

    testHarness.getOutput().size() should be(0)

    testHarness.processElement(TestEvent(2L, 1, 0), 100L)
    testHarness.getOutput().size() shouldNot be(0)
  }
