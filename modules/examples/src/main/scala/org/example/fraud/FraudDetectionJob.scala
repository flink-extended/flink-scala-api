package org.example.fraud

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.File

import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.state.api.SavepointReader

import org.example.Transaction
import org.example.TransactionsSource
import org.example.Alert
import org.example.AlertSink

import Givens.given

@main def runningAvg(sleepBeforeEmit: Long) =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val transactions = env
    .addSource(TransactionsSource.iterator(sleepBeforeEmit))
    .name("transactions")

  transactions
    .flatMap(t => if t.amount < 1.0d then List(t, t) else List(t))
    .keyBy(_.accountId)
    .map(RunningAverage())
    .keyBy(_ => "all")
    .reduce { (a, b) =>
      val runningAvg = (a._2 + b._2) / 2
      println(s"average ${Thread.currentThread.getName}: $runningAvg")
      b._1 -> runningAvg
    }
    .name("fraud-detector")

  env.execute("Fraud Detection")

@main def FraudDetectionJob(sleepBeforeEmit: Long) =
  val conf = Configuration()
  conf.setString("state.savepoints.dir", "file:///tmp/savepoints")
  conf.setString(
    "execution.checkpointing.externalized-checkpoint-retention",
    "RETAIN_ON_CANCELLATION"
  )
  conf.setString("execution.checkpointing.interval", "3s")
  conf.setString("execution.checkpointing.min-pause", "3s")
  conf.setString("state.backend", "filesystem")

  val env = StreamExecutionEnvironment.getExecutionEnvironment //.createLocalEnvironmentWithWebUI(conf)

  val transactions = env
    .addSource(TransactionsSource.iterator(sleepBeforeEmit))
    .name("transactions")
    .union()

  val alerts = transactions
    .keyBy(_.accountId)
    .process(FraudDetector())
    .uid("fraud-state")
    .name("fraud-detector")

  alerts
    .addSink(AlertSink())
    .name("send-alerts")

  env.execute("Fraud Detection")

@main def fraudDetectionState() =
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val savepoint = SavepointReader.read(env.getJavaEnv, "///tmp/savepoints/savepoint-827976-a94a8feb6c07",
    HashMapStateBackend())
  val keyedState = savepoint.readKeyedState("fraud-state", ReaderFunction(), TypeInformation.of(classOf[Long]), keyedStateInfo)
  keyedState.print()
  env.execute()

case class MaxTransaction(amount: Double, timestamp: Long)

class MaxAggregate
    extends AggregateFunction[Transaction, MaxTransaction, MaxTransaction]:
  override def createAccumulator(): MaxTransaction = MaxTransaction(0d, 0L)

  override def add(value: Transaction, accumulator: MaxTransaction): MaxTransaction =
    if value.amount > accumulator._1 then
      MaxTransaction(value.amount, value.timestamp)
    else accumulator

  override def getResult(accumulator: MaxTransaction): MaxTransaction =
    accumulator

  override def merge(a: MaxTransaction, b: MaxTransaction): MaxTransaction =
    if a._1 >= b._1 then a else b

@main def maxAmount =
  val env =
    StreamExecutionEnvironment.getExecutionEnvironment.enableCheckpointing(
      10_000L
    )

  env.getCheckpointConfig.setCheckpointStorage(
    s"file://${File(".").getAbsolutePath}/max-amount-checkpoint"
  )

  val transactions = env
    .addSource(TransactionsSource.iterator(100))
    .name("transactions")

  transactions
    .keyBy(_.accountId)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .reduce((a, b) => if a.amount < b.amount then b else a)
    // .aggregate(MaxAggregate())
    .name("windowed-max")
    .print()

  env.execute("Max Amount Transaction")
