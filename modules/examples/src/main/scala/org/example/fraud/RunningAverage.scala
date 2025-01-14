package org.example.fraud

import org.apache.flinkx.api._
import org.apache.flinkx.api.serializers._

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.example.Transaction

class RunningAverage extends RichMapFunction[Transaction, (Transaction, Double)]:

  given tranTypeInfo: TypeInformation[Transaction] =
    TypeInformation.of(classOf[Transaction])

  @transient lazy val runningAvg = getRuntimeContext.getState(
    ValueStateDescriptor(
      "running-average",
      classOf[Double],
      0d
    )
  )

  @transient lazy val count = getRuntimeContext.getState(
    ValueStateDescriptor("count", classOf[Int], 0)
  )

  private def threadName = Thread.currentThread.getName
  override def open(config: Configuration): Unit =
    println(s"open map: $threadName")

  override def map(t: Transaction): (Transaction, Double) =
    Option(count.value) match
      case Some(cnt) => count.update(cnt + 1)
      case _         => ()

    Option(runningAvg.value) match
      case Some(avg) => runningAvg.update((avg + t.amount) / count.value)
      case _         => ()

    (t, runningAvg.value)
