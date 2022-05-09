package io.findify.flink.api

import io.findify.flink.ClosureCleaner
import io.findify.flink.api.function.{AllWindowFunction, ProcessAllWindowFunction}
import io.findify.flink.api.function.util.{
  ScalaAllWindowFunction,
  ScalaAllWindowFunctionWrapper,
  ScalaProcessAllWindowFunctionWrapper,
  ScalaReduceFunction
}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{AllWindowedStream, DataStream}
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

trait AllWindowedStreamOps[T, W <: Window] {
  def stream: AllWindowedStream[T, W]

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
  def reduce(function: ReduceFunction[T]): DataStream[T] = {
    stream.reduce(ClosureCleaner.clean(function))
  }

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
    val cleanFun = ClosureCleaner.clean(function)
    val reducer  = new ScalaReduceFunction[T](cleanFun)
    reduce(reducer)
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
      preAggregator: ReduceFunction[T],
      windowFunction: AllWindowFunction[T, R, W]
  ): DataStream[R] = {

    val cleanedReducer        = ClosureCleaner.clean(preAggregator)
    val cleanedWindowFunction = ClosureCleaner.clean(windowFunction)

    val applyFunction = new ScalaAllWindowFunctionWrapper[T, R, W](cleanedWindowFunction)

    val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.reduce(cleanedReducer, applyFunction, returnType)
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
      windowFunction: (W, Iterable[T], Collector[R]) => Unit
  ): DataStream[R] = {

    val cleanReducer        = ClosureCleaner.clean(preAggregator)
    val cleanWindowFunction = ClosureCleaner.clean(windowFunction)

    val reducer       = new ScalaReduceFunction[T](cleanReducer)
    val applyFunction = new ScalaAllWindowFunction[T, R, W](cleanWindowFunction)

    val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.reduce(reducer, applyFunction, returnType)
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window
    * for each key individually. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Arriving data is pre-aggregated using the given pre-aggregation reducer.
    *
    * @param preAggregator
    *   The reduce function that is used for pre-aggregation
    * @param windowFunction
    *   The process window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def reduce[R: TypeInformation](
      preAggregator: ReduceFunction[T],
      windowFunction: ProcessAllWindowFunction[T, R, W]
  ): DataStream[R] = {

    val cleanedReducer        = ClosureCleaner.clean(preAggregator)
    val cleanedWindowFunction = ClosureCleaner.clean(windowFunction)

    val applyFunction = new ScalaProcessAllWindowFunctionWrapper[T, R, W](cleanedWindowFunction)

    val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.reduce(cleanedReducer, applyFunction, returnType)
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window
    * for each key individually. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Arriving data is pre-aggregated using the given pre-aggregation reducer.
    *
    * @param preAggregator
    *   The reduce function that is used for pre-aggregation
    * @param windowFunction
    *   The process window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def reduce[R: TypeInformation](
      preAggregator: (T, T) => T,
      windowFunction: ProcessAllWindowFunction[T, R, W]
  ): DataStream[R] = {

    if (preAggregator == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    if (windowFunction == null) {
      throw new NullPointerException("WindowApply function must not be null.")
    }

    val cleanReducer        = ClosureCleaner.clean(preAggregator)
    val cleanWindowFunction = ClosureCleaner.clean(windowFunction)

    val reducer       = new ScalaReduceFunction[T](cleanReducer)
    val applyFunction = new ScalaProcessAllWindowFunctionWrapper[T, R, W](cleanWindowFunction)

    val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.reduce(reducer, applyFunction, returnType)
  }

  /** Applies the given aggregation function to each window. The aggregation function is called for each element,
    * aggregating values incrementally and keeping the state to one accumulator per window.
    *
    * @param aggregateFunction
    *   The aggregation function.
    * @return
    *   The data stream that is the result of applying the aggregate function to the window.
    */
  def aggregate[ACC: TypeInformation, R: TypeInformation](
      aggregateFunction: AggregateFunction[T, ACC, R]
  ): DataStream[R] = {

    val accumulatorType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]
    val resultType: TypeInformation[R]        = implicitly[TypeInformation[R]]

    stream.aggregate(ClosureCleaner.clean(aggregateFunction), accumulatorType, resultType)
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window
    * for each key individually. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Arriving data is pre-aggregated using the given aggregation function.
    *
    * @param preAggregator
    *   The aggregation function that is used for pre-aggregation
    * @param windowFunction
    *   The window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
      preAggregator: AggregateFunction[T, ACC, V],
      windowFunction: AllWindowFunction[V, R, W]
  ): DataStream[R] = {
    val cleanedPreAggregator  = ClosureCleaner.clean(preAggregator)
    val cleanedWindowFunction = ClosureCleaner.clean(windowFunction)

    val applyFunction = new ScalaAllWindowFunctionWrapper[V, R, W](cleanedWindowFunction)

    val accumulatorType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]
    val resultType: TypeInformation[R]        = implicitly[TypeInformation[R]]

    stream.aggregate(cleanedPreAggregator, applyFunction, accumulatorType, resultType)
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window
    * for each key individually. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Arriving data is pre-aggregated using the given aggregation function.
    *
    * @param preAggregator
    *   The aggregation function that is used for pre-aggregation
    * @param windowFunction
    *   The process window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
      preAggregator: AggregateFunction[T, ACC, V],
      windowFunction: ProcessAllWindowFunction[V, R, W]
  ): DataStream[R] = {
    val cleanedPreAggregator  = ClosureCleaner.clean(preAggregator)
    val cleanedWindowFunction = ClosureCleaner.clean(windowFunction)

    val applyFunction = new ScalaProcessAllWindowFunctionWrapper[V, R, W](cleanedWindowFunction)

    val accumulatorType: TypeInformation[ACC]     = implicitly[TypeInformation[ACC]]
    val aggregationResultType: TypeInformation[V] = implicitly[TypeInformation[V]]
    val resultType: TypeInformation[R]            = implicitly[TypeInformation[R]]

    stream.aggregate(cleanedPreAggregator, applyFunction, accumulatorType, aggregationResultType, resultType)
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window.
    * The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Arriving data is pre-aggregated using the given aggregation function.
    *
    * @param preAggregator
    *   The aggregation function that is used for pre-aggregation
    * @param windowFunction
    *   The window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
      preAggregator: AggregateFunction[T, ACC, V],
      windowFunction: (W, Iterable[V], Collector[R]) => Unit
  ): DataStream[R] = {

    val cleanPreAggregator  = ClosureCleaner.clean(preAggregator)
    val cleanWindowFunction = ClosureCleaner.clean(windowFunction)

    val applyFunction = new ScalaAllWindowFunction[V, R, W](cleanWindowFunction)

    val accumulatorType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]
    val resultType: TypeInformation[R]        = implicitly[TypeInformation[R]]

    stream.aggregate(cleanPreAggregator, applyFunction, accumulatorType, resultType)
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window
    * for each key individually. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Not that this function requires that all data in the windows is buffered until the window is evaluated, as the
    * function provides no means of pre-aggregation.
    *
    * @param function
    *   The process window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def process[R: TypeInformation](function: ProcessAllWindowFunction[T, R, W]): DataStream[R] = {

    val cleanedFunction = ClosureCleaner.clean(function)
    val javaFunction    = new ScalaProcessAllWindowFunctionWrapper[T, R, W](cleanedFunction)

    stream.process(javaFunction, implicitly[TypeInformation[R]])
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window
    * for each key individually. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Not that this function requires that all data in the windows is buffered until the window is evaluated, as the
    * function provides no means of pre-aggregation.
    *
    * @param function
    *   The window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def apply[R: TypeInformation](function: AllWindowFunction[T, R, W]): DataStream[R] = {

    val cleanedFunction = ClosureCleaner.clean(function)
    val javaFunction    = new ScalaAllWindowFunctionWrapper[T, R, W](cleanedFunction)

    stream.apply(javaFunction, implicitly[TypeInformation[R]])
  }

  /** Applies the given window function to each window. The window function is called for each evaluation of the window
    * for each key individually. The output of the window function is interpreted as a regular non-windowed stream.
    *
    * Not that this function requires that all data in the windows is buffered until the window is evaluated, as the
    * function provides no means of pre-aggregation.
    *
    * @param function
    *   The window function.
    * @return
    *   The data stream that is the result of applying the window function to the window.
    */
  def apply[R: TypeInformation](function: (W, Iterable[T], Collector[R]) => Unit): DataStream[R] = {

    val cleanedFunction = ClosureCleaner.clean(function)
    val applyFunction   = new ScalaAllWindowFunction[T, R, W](cleanedFunction)

    stream.apply(applyFunction, implicitly[TypeInformation[R]])
  }

}
