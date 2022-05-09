package io.findify.flink.api

import io.findify.flink.ClosureCleaner
import io.findify.flink.api.function.ProcessWindowFunction
import io.findify.flink.api.function.util.{
  ScalaProcessWindowFunctionWrapper,
  ScalaReduceFunction,
  ScalaWindowFunction,
  ScalaWindowFunctionWrapper
}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{DataStream, WindowedStream}
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

trait WindowedStreamOps[T, K, W <: Window] {
  def stream: WindowedStream[T, K, W]

  /** Applies a reduce function to the window. The window function is called for each evaluation of the window for each
    * key individually. The output of the reduce function is interpreted as a regular non-windowed stream.
    *
    * This window will try and pre-aggregate data as much as the window policies permit. For example, tumbling time
    * windows can perfectly pre-aggregate the data, meaning that only one element per key is stored. Sliding time
    * windows will pre-aggregate on the granularity of the slide interval, so a few elements are stored per key (one per
    * slide interval). Custom windows may not be able to pre-aggregate, or may need to store extra values in an
    * aggregation tree.
    *
    * @param function
    *   The reduce function.
    * @return
    *   The data stream that is the result of applying the reduce function to the window.
    */
  def reduce(function: (T, T) => T): DataStream[T] = {
    if (function == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    val cleanFun = ClosureCleaner.clean(function)
    val reducer  = new ScalaReduceFunction[T](cleanFun)
    stream.reduce(reducer)
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window
    * for each key individually. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Arriving data is pre-aggregated using the given pre-aggregation reducer.
    *
    * @param preAggregator
    *   The reduce function that is used for pre-aggregation
    * @param function
    *   The window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def reduce[R: TypeInformation](
      preAggregator: ReduceFunction[T],
      function: WindowFunction[T, R, K, W]
  ): DataStream[R] = {

    val cleanedPreAggregator  = ClosureCleaner.clean(preAggregator)
    val cleanedWindowFunction = ClosureCleaner.clean(function)

    val applyFunction = new ScalaWindowFunctionWrapper[T, R, K, W](cleanedWindowFunction)

    val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.reduce(cleanedPreAggregator, applyFunction, resultType)
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window
    * for each key individually. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Arriving data is pre-aggregated using the given pre-aggregation reducer.
    *
    * @param preAggregator
    *   The reduce function that is used for pre-aggregation
    * @param windowFunction
    *   The window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def reduce[R: TypeInformation](
      preAggregator: (T, T) => T,
      windowFunction: (K, W, Iterable[T], Collector[R]) => Unit
  ): DataStream[R] = {
    val cleanReducer        = ClosureCleaner.clean(preAggregator)
    val cleanWindowFunction = ClosureCleaner.clean(windowFunction)

    val reducer       = new ScalaReduceFunction[T](cleanReducer)
    val applyFunction = new ScalaWindowFunction[T, R, K, W](cleanWindowFunction)

    stream.reduce(reducer, applyFunction, implicitly[TypeInformation[R]])
  }

  /** Applies the given reduce function to each window. The window reduced value is then passed as input of the window
    * function. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * @param preAggregator
    *   The reduce function that is used for pre-aggregation
    * @param function
    *   The process window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def reduce[R: TypeInformation](
      preAggregator: (T, T) => T,
      function: ProcessWindowFunction[T, R, K, W]
  ): DataStream[R] = {

    val cleanedPreAggregator  = ClosureCleaner.clean(preAggregator)
    val cleanedWindowFunction = ClosureCleaner.clean(function)

    val reducer       = new ScalaReduceFunction[T](cleanedPreAggregator)
    val applyFunction = new ScalaProcessWindowFunctionWrapper[T, R, K, W](cleanedWindowFunction)

    val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.reduce(reducer, applyFunction, resultType)
  }

  /** Applies the given reduce function to each window. The window reduced value is then passed as input of the window
    * function. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * @param preAggregator
    *   The reduce function that is used for pre-aggregation
    * @param function
    *   The process window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def reduce[R: TypeInformation](
      preAggregator: ReduceFunction[T],
      function: ProcessWindowFunction[T, R, K, W]
  ): DataStream[R] = {

    val cleanedPreAggregator  = ClosureCleaner.clean(preAggregator)
    val cleanedWindowFunction = ClosureCleaner.clean(function)

    val applyFunction = new ScalaProcessWindowFunctionWrapper[T, R, K, W](cleanedWindowFunction)

    val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.reduce(cleanedPreAggregator, applyFunction, resultType)
  }

}
