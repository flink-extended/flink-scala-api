package org.apache.flinkx.api

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies
import org.apache.flinkx.api.async._
import org.apache.flinkx.api.serializers.intInfo
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class AsyncDataStreamTest extends AnyFlatSpec with Matchers with IntegrationTest with BeforeAndAfter {
  import AsyncDataStreamTest._

  private val source              = env.fromElements(1, 2, 3)
  private val timeout             = 1000L
  private val timeUnit            = TimeUnit.MILLISECONDS
  private val asyncRetryStrategy  = new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder[Int](3, 10).build()
  private val myRichAsyncFunction = new MyRichAsyncFunction
  private val anonymousAsyncFunction: (Int, ResultFuture[Int]) => Unit = (input: Int, collector: ResultFuture[Int]) => {
    Future { collector.complete(Seq(input * 2)) }
  }

  before {
    env.setParallelism(1)
  }

  it should "apply rich async function to the stream in ordered manner" in {
    AsyncDataStream
      .orderedWait(source, myRichAsyncFunction, timeout, timeUnit)
      .collect() should contain theSameElementsInOrderAs List(2, 4, 6)
  }

  it should "apply rich async function to the stream in ordered manner with retry policy" in {
    AsyncDataStream
      .orderedWaitWithRetry(source, myRichAsyncFunction, timeout, timeUnit, asyncRetryStrategy)
      .collect() should contain theSameElementsInOrderAs List(2, 4, 6)
  }

  it should "apply rich async function to the stream in unordered manner" in {
    AsyncDataStream
      .unorderedWait(source, myRichAsyncFunction, timeout, timeUnit)
      .collect() should contain theSameElementsAs List(2, 4, 6)
  }

  it should "apply rich async function to the stream in unordered manner with retry policy" in {
    AsyncDataStream
      .orderedWaitWithRetry(source, timeout, timeUnit, asyncRetryStrategy) { anonymousAsyncFunction }
      .collect() should contain theSameElementsAs List(2, 4, 6)
  }

  it should "apply anon async function to the stream in ordered manner" in {
    AsyncDataStream
      .orderedWait(source, timeout, timeUnit) { anonymousAsyncFunction }
      .collect() should contain theSameElementsInOrderAs List(2, 4, 6)
  }

  it should "apply anon async function to the stream in ordered manner with retry policy" in {
    AsyncDataStream
      .orderedWaitWithRetry(source, timeout, timeUnit, asyncRetryStrategy) { anonymousAsyncFunction }
      .collect() should contain theSameElementsInOrderAs List(2, 4, 6)
  }

  it should "apply anon async function to the stream in unordered manner" in {
    AsyncDataStream
      .unorderedWait(source, timeout, timeUnit) { anonymousAsyncFunction }
      .collect() should contain theSameElementsAs List(2, 4, 6)
  }

  it should "apply anon async function to the stream in unordered manner with retry policy" in {
    AsyncDataStream
      .orderedWaitWithRetry(source, timeout, timeUnit, asyncRetryStrategy) { anonymousAsyncFunction }
      .collect() should contain theSameElementsAs List(2, 4, 6)
  }
}

object AsyncDataStreamTest {
  class MyRichAsyncFunction extends RichAsyncFunction[Int, Int] {
    override def open(openContext: OpenContext): Unit = {
      assert(getRuntimeContext.getTaskInfo.getNumberOfParallelSubtasks == 1)
    }

    override def asyncInvoke(input: Int, resultFuture: ResultFuture[Int]): Unit = Future {
      resultFuture.complete(Seq(input * 2))
    }
  }
}
