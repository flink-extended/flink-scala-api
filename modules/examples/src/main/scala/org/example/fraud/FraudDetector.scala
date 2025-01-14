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

import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.state.api.functions.KeyedStateReaderFunction.Context
import org.apache.flink.util.Collector

import org.example.{Alert, Transaction}

import org.slf4j.LoggerFactory
import scala.concurrent.duration.*

import FraudDetector.*

object Givens:
  given tranTypeInfo: TypeInformation[Transaction] =
    deriveTypeInformation[Transaction]
  given alertTypeInfo: TypeInformation[Alert] =
    TypeInformation.of(classOf[Alert])
  given keyedStateInfo: TypeInformation[KeyedFraudState] =
    TypeInformation.of(classOf[KeyedFraudState])

case class FraudStateVars(
    flagState: ValueState[Boolean],
    timerState: ValueState[Long],
    lastTransaction: ValueState[Transaction]
):
  def clear(): Unit =
    flagState.clear()
    timerState.clear()

object FraudDetector:
  val SmallAmount     = 1.00
  val LargeAmount     = 500.00
  val OneMinute: Long = 1.minute.toMillis

  def readState(context: RuntimeContext): FraudStateVars =
    FraudStateVars(
      context.getState(
        ValueStateDescriptor("flag", boolInfo)
      ),
      context.getState(
        ValueStateDescriptor("timer-state", longInfo)
      ),
      context.getState(
        ValueStateDescriptor("last-transaction", Givens.tranTypeInfo)
      )
    )

case class KeyedFraudState(key: Long, state: FraudStateVars)

class ReaderFunction extends KeyedStateReaderFunction[Long, KeyedFraudState]:
  var fraudState: FraudStateVars = _

  override def open(parameters: Configuration): Unit =
    fraudState = readState(getRuntimeContext)

  override def readKey(
      key: Long,
      ctx: Context,
      out: Collector[KeyedFraudState]
  ): Unit =
    out.collect(KeyedFraudState(key, fraudState))

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert]:
  @transient lazy val logger = LoggerFactory.getLogger(classOf[FraudDetector])

  @transient var fraudState: FraudStateVars = _

  override def open(parameters: Configuration): Unit =
    fraudState = readState(getRuntimeContext)
    logger.info(s"Loaded last transaction: ${fraudState.lastTransaction}")

  @throws[Exception]
  def processElement(
      transaction: Transaction,
      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
      collector: Collector[Alert]
  ): Unit =
    // Get the current state for the current key
    Option(fraudState.flagState.value).foreach { _ =>
      if transaction.amount > FraudDetector.LargeAmount then
        // Output an alert downstream
        val alert = Alert(transaction.accountId)
        collector.collect(alert)
        logger.info(s"Fraudulent transaction: $transaction")

      // Clean up our state
      cleanUp(context)
    }

    if transaction.amount < FraudDetector.SmallAmount then
      // set the flag to true
      fraudState.flagState.update(true)

      // set the timer and timer state
      val timer =
        context.timerService.currentProcessingTime + FraudDetector.OneMinute
      context.timerService.registerProcessingTimeTimer(timer)
      fraudState.timerState.update(timer)
      logger.info(s"small amount: ${transaction.amount}")

      fraudState.lastTransaction.update(transaction)

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
      out: Collector[Alert]
  ): Unit =
    // remove flag after 1 minute, assuming that attacker makes fraudulent transactions within a minute
    fraudState.clear()

  @throws[Exception]
  private def cleanUp(
      ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context
  ): Unit =
    // delete timer
    val timer = fraudState.timerState.value
    ctx.timerService.deleteProcessingTimeTimer(timer)

    // clean up all states
    fraudState.clear()
