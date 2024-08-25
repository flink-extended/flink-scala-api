package org.example

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit
import java.time.Duration

case class TestCommand(timestamp: Long, bag: List[String] = Nil)

class ConnectedStreamsTest extends AnyFlatSpec with Matchers:

  it should "process in random order" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    
    val commands = Seq(TestCommand(1), TestCommand(2), TestCommand(3))
    val events = Seq(
      TestEvent(2L, 1, 0),
      TestEvent(2L, 2, 0),
      TestEvent(2L, 3, 0)
    )
    def strategy[T] = WatermarkStrategy
      .forBoundedOutOfOrderness[T](Duration.ofSeconds(1000))

    env
      .fromCollection(commands)
      .assignTimestampsAndWatermarks(
        strategy
          .withTimestampAssigner(
            (cmd: TestCommand, streamRecordTimestamp: Long) => cmd.timestamp
          )
      )
      .connect(
        env
          .fromCollection(events)
          .assignTimestampsAndWatermarks(
            strategy
              .withTimestampAssigner(
                (event: TestEvent, streamRecordTimestamp: Long) =>
                  event.timestamp
              )
          )
      )
      .process {
        new CoProcessFunction[TestCommand, TestEvent, String]:

          override def processElement1(
              value: TestCommand,
              ctx: CoProcessFunction[
                org.example.TestCommand,
                org.example.TestEvent,
                String
              ]#Context,
              out: Collector[String]
          ): Unit =
            out.collect(s"cmd: ${value.timestamp}")

          override def processElement2(
              value: TestEvent,
              ctx: CoProcessFunction[
                org.example.TestCommand,
                org.example.TestEvent,
                String
              ]#Context,
              out: Collector[String]
          ): Unit =
            out.collect(s"event: ${value.timestamp}")
      }
      .print()

    env.execute()
  }
