package org.example

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

@main def ConnectedStreams =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  given tranTypeInfo: TypeInformation[Transaction] = deriveTypeInformation

  val control = env
    .addSource(TransactionsSource.iterator)
    .keyBy(_.accountId)

  val streamOfWords = env
    .addSource(TransactionsSource.iterator)
    .keyBy(_.accountId)
  
  control
    .connect(streamOfWords)
    .flatMap(ControlFunction())
    .print()

  env.execute()

class ControlFunction
    extends RichCoFlatMapFunction[Transaction, Transaction, Transaction]:

  @transient lazy val state: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor(
      "joined-transaction",
      classOf[Double]
    )
  )

  override def flatMap1(
      t: Transaction,
      out: Collector[Transaction]
  ): Unit =
    sumUp(t, out)

  override def flatMap2(
      t: Transaction,
      out: Collector[Transaction]
  ): Unit =
    sumUp(t, out)

  private def sumUp(t: Transaction, out: Collector[Transaction]): Unit =
    Option(state.value()) match
      case Some(v) =>
        out.collect(t.copy(amount = t.amount + v))
        state.clear()
      case None =>
        state.update(t.amount)
