package org.apache.flinkx.api

import org.apache.flinkx.api.async.{
  AsyncFunction,
  JavaResultFutureWrapper,
  ResultFuture,
  RichAsyncFunction,
  ScalaRichAsyncFunctionWrapper
}
import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.apache.flink.streaming.api.functions.async.{
  AsyncRetryStrategy => JavaAsyncRetryStrategy,
  AsyncFunction => JavaAsyncFunction,
  ResultFuture => JavaResultFuture
}
import org.apache.flink.util.Preconditions
import ScalaStreamOps._

import scala.concurrent.duration.TimeUnit

/** A helper class to apply [[AsyncFunction]] to a data stream.
  *
  * Example:
  * {{{
  *   val input: DataStream[String] = ...
  *   val asyncFunction: (String, ResultFuture[String]) => Unit = ...
  *
  *   AsyncDataStream.orderedWait(input, asyncFunction, timeout, TimeUnit.MILLISECONDS, 100)
  * }}}
  */
@PublicEvolving
object AsyncDataStream {

  private val DEFAULT_QUEUE_CAPACITY = 100

  /** Apply an asynchronous function on the input data stream. The output order is only maintained with respect to
    * watermarks. Stream records which lie between the same two watermarks, can be re-ordered.
    *
    * @param input
    *   to apply the async function on
    * @param asyncFunction
    *   to use
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param capacity
    *   of the operator which is equivalent to the number of concurrent asynchronous operations
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def unorderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int
  ): DataStream[OUT] = {

    val javaAsyncFunction = wrapAsJavaAsyncFunction(asyncFunction)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .unorderedWait[IN, OUT](input.javaStream, javaAsyncFunction, timeout, timeUnit, capacity)
        .returns(outType)
    )
  }

  /** Apply an asynchronous function on the input data stream. The output order is only maintained with respect to
    * watermarks. Stream records which lie between the same two watermarks, can be re-ordered.
    *
    * @param input
    *   to apply the async function on
    * @param asyncFunction
    *   to use
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def unorderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit
  ): DataStream[OUT] = {

    unorderedWait(input, asyncFunction, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY)
  }

  /** Apply an asynchronous function on the input data stream. The output order is only maintained with respect to
    * watermarks. Stream records which lie between the same two watermarks, can be re-ordered.
    *
    * @param input
    *   to apply the async function on
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param capacity
    *   of the operator which is equivalent to the number of concurrent asynchronous operations
    * @param asyncFunction
    *   to use
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def unorderedWait[IN, OUT: TypeInformation](input: DataStream[IN], timeout: Long, timeUnit: TimeUnit, capacity: Int)(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit
  ): DataStream[OUT] = {

    Preconditions.checkNotNull(asyncFunction)

    val cleanAsyncFunction = input.executionEnvironment.scalaClean(asyncFunction)

    val func = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {

        cleanAsyncFunction(input, new JavaResultFutureWrapper[OUT](resultFuture))
      }
    }

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream.unorderedWait[IN, OUT](input.javaStream, func, timeout, timeUnit, capacity).returns(outType)
    )
  }

  /** Apply an asynchronous function on the input data stream. The output order is only maintained with respect to
    * watermarks. Stream records which lie between the same two watermarks, can be re-ordered.
    *
    * @param input
    *   to apply the async function on
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param asyncFunction
    *   to use
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def unorderedWait[IN, OUT: TypeInformation](input: DataStream[IN], timeout: Long, timeUnit: TimeUnit)(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit
  ): DataStream[OUT] = {
    unorderedWait(input, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY)(asyncFunction)
  }

  /** Apply an asynchronous function on the input data stream. The output order is the same as the input order of the
    * elements.
    *
    * @param input
    *   to apply the async function on
    * @param asyncFunction
    *   to use
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param capacity
    *   of the operator which is equivalent to the number of concurrent asynchronous operations
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def orderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int
  ): DataStream[OUT] = {

    val javaAsyncFunction = wrapAsJavaAsyncFunction(asyncFunction)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .orderedWait[IN, OUT](input.javaStream, javaAsyncFunction, timeout, timeUnit, capacity)
        .returns(outType)
    )
  }

  /** Apply an asynchronous function on the input data stream. The output order is the same as the input order of the
    * elements.
    *
    * @param input
    *   to apply the async function on
    * @param asyncFunction
    *   to use
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def orderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit
  ): DataStream[OUT] = {
    orderedWait(input, asyncFunction, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY)
  }

  /** Apply an asynchronous function on the input data stream. The output order is the same as the input order of the
    * elements.
    *
    * @param input
    *   to apply the async function on
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param capacity
    *   of the operator which is equivalent to the number of concurrent asynchronous operations
    * @param asyncFunction
    *   to use
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def orderedWait[IN, OUT: TypeInformation](input: DataStream[IN], timeout: Long, timeUnit: TimeUnit, capacity: Int)(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit
  ): DataStream[OUT] = {

    Preconditions.checkNotNull(asyncFunction)

    val cleanAsyncFunction = input.executionEnvironment.scalaClean(asyncFunction)

    val func = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {
        cleanAsyncFunction(input, new JavaResultFutureWrapper[OUT](resultFuture))
      }
    }

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream.orderedWait[IN, OUT](input.javaStream, func, timeout, timeUnit, capacity).returns(outType)
    )
  }

  /** Apply an asynchronous function on the input data stream. The output order is the same as the input order of the
    * elements.
    *
    * @param input
    *   to apply the async function on
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param asyncFunction
    *   to use
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def orderedWait[IN, OUT: TypeInformation](input: DataStream[IN], timeout: Long, timeUnit: TimeUnit)(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit
  ): DataStream[OUT] = {

    orderedWait(input, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY)(asyncFunction)
  }

  /** Apply an asynchronous function on the input data stream. The output order is only maintained with respect to
    * watermarks. Stream records which lie between the same two watermarks, can be re-ordered.
    *
    * @param input
    *   to apply the async function on
    * @param asyncFunction
    *   to use
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param capacity
    *   of the operator which is equivalent to the number of concurrent asynchronous operations
    * @param asyncRetryStrategy
    *   The strategy of reattempt async i/o operation that can be triggered
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def unorderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int,
      asyncRetryStrategy: JavaAsyncRetryStrategy[OUT]
  ): DataStream[OUT] = {

    val javaAsyncFunction = wrapAsJavaAsyncFunction(asyncFunction)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .unorderedWaitWithRetry[IN, OUT](
          input.javaStream,
          javaAsyncFunction,
          timeout,
          timeUnit,
          capacity,
          asyncRetryStrategy
        )
        .returns(outType)
    )
  }

  /** Apply an asynchronous function on the input data stream. The output order is only maintained with respect to
    * watermarks. Stream records which lie between the same two watermarks, can be re-ordered.
    *
    * @param input
    *   to apply the async function on
    * @param asyncFunction
    *   to use
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param asyncRetryStrategy
    *   The strategy of reattempt async i/o operation that can be triggered
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def unorderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      asyncRetryStrategy: JavaAsyncRetryStrategy[OUT]
  ): DataStream[OUT] = {

    unorderedWaitWithRetry(input, asyncFunction, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY, asyncRetryStrategy)
  }

  /** Apply an asynchronous function on the input data stream. The output order is only maintained with respect to
    * watermarks. Stream records which lie between the same two watermarks, can be re-ordered.
    *
    * @param input
    *   to apply the async function on
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param capacity
    *   of the operator which is equivalent to the number of concurrent asynchronous operations
    * @param asyncRetryStrategy
    *   The strategy of reattempt async i/o operation that can be triggered
    * @param asyncFunction
    *   to use
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def unorderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int,
      asyncRetryStrategy: JavaAsyncRetryStrategy[OUT]
  )(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit
  ): DataStream[OUT] = {

    Preconditions.checkNotNull(asyncFunction)

    val cleanAsyncFunction = input.executionEnvironment.scalaClean(asyncFunction)

    val func = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {

        cleanAsyncFunction(input, new JavaResultFutureWrapper[OUT](resultFuture))
      }
    }

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .unorderedWaitWithRetry[IN, OUT](input.javaStream, func, timeout, timeUnit, capacity, asyncRetryStrategy)
        .returns(outType)
    )
  }

  /** Apply an asynchronous function on the input data stream. The output order is only maintained with respect to
    * watermarks. Stream records which lie between the same two watermarks, can be re-ordered.
    *
    * @param input
    *   to apply the async function on
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param asyncRetryStrategy
    *   The strategy of reattempt async i/o operation that can be triggered
    * @param asyncFunction
    *   to use
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def unorderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      asyncRetryStrategy: JavaAsyncRetryStrategy[OUT]
  )(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit
  ): DataStream[OUT] = {
    unorderedWaitWithRetry(input, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY, asyncRetryStrategy)(asyncFunction)
  }

  /** Apply an asynchronous function on the input data stream. The output order is the same as the input order of the
    * elements.
    *
    * @param input
    *   to apply the async function on
    * @param asyncFunction
    *   to use
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param capacity
    *   of the operator which is equivalent to the number of concurrent asynchronous operations
    * @param asyncRetryStrategy
    *   The strategy of reattempt async i/o operation that can be triggered
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def orderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int,
      asyncRetryStrategy: JavaAsyncRetryStrategy[OUT]
  ): DataStream[OUT] = {

    val javaAsyncFunction = wrapAsJavaAsyncFunction(asyncFunction)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .orderedWaitWithRetry[IN, OUT](
          input.javaStream,
          javaAsyncFunction,
          timeout,
          timeUnit,
          capacity,
          asyncRetryStrategy
        )
        .returns(outType)
    )
  }

  /** Apply an asynchronous function on the input data stream. The output order is the same as the input order of the
    * elements.
    *
    * @param input
    *   to apply the async function on
    * @param asyncFunction
    *   to use
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param asyncRetryStrategy
    *   The strategy of reattempt async i/o operation that can be triggered
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def orderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      asyncRetryStrategy: JavaAsyncRetryStrategy[OUT]
  ): DataStream[OUT] = {
    orderedWaitWithRetry(input, asyncFunction, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY, asyncRetryStrategy)
  }

  /** Apply an asynchronous function on the input data stream. The output order is the same as the input order of the
    * elements.
    *
    * @param input
    *   to apply the async function on
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param capacity
    *   of the operator which is equivalent to the number of concurrent asynchronous operations
    * @param asyncRetryStrategy
    *   The strategy of reattempt async i/o operation that can be triggered
    * @param asyncFunction
    *   to use
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def orderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int,
      asyncRetryStrategy: JavaAsyncRetryStrategy[OUT]
  )(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit
  ): DataStream[OUT] = {

    Preconditions.checkNotNull(asyncFunction)

    val cleanAsyncFunction = input.executionEnvironment.scalaClean(asyncFunction)

    val func = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {
        cleanAsyncFunction(input, new JavaResultFutureWrapper[OUT](resultFuture))
      }
    }

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .orderedWaitWithRetry[IN, OUT](input.javaStream, func, timeout, timeUnit, capacity, asyncRetryStrategy)
        .returns(outType)
    )
  }

  /** Apply an asynchronous function on the input data stream. The output order is the same as the input order of the
    * elements.
    *
    * @param input
    *   to apply the async function on
    * @param timeout
    *   for the asynchronous operation to complete
    * @param timeUnit
    *   of the timeout
    * @param asyncRetryStrategy
    *   The strategy of reattempt async i/o operation that can be triggered
    * @param asyncFunction
    *   to use
    * @tparam IN
    *   Type of the input record
    * @tparam OUT
    *   Type of the output record
    * @return
    *   the resulting stream containing the asynchronous results
    */
  def orderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      asyncRetryStrategy: JavaAsyncRetryStrategy[OUT]
  )(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit
  ): DataStream[OUT] = {

    orderedWaitWithRetry(input, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY, asyncRetryStrategy)(asyncFunction)
  }

  private def wrapAsJavaAsyncFunction[IN, OUT: TypeInformation](
      asyncFunction: AsyncFunction[IN, OUT]
  ): JavaAsyncFunction[IN, OUT] = asyncFunction match {
    case richAsyncFunction: RichAsyncFunction[IN, OUT] =>
      new ScalaRichAsyncFunctionWrapper[IN, OUT](richAsyncFunction)
    case _ =>
      new JavaAsyncFunction[IN, OUT] {
        override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {
          asyncFunction.asyncInvoke(input, new JavaResultFutureWrapper[OUT](resultFuture))
        }

        override def timeout(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {
          asyncFunction.timeout(input, new JavaResultFutureWrapper[OUT](resultFuture))
        }
      }
  }
}
