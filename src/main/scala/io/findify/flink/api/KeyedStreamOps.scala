package io.findify.flink.api

import io.findify.flink.ClosureCleaner
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{DataStream, KeyedStream}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

trait KeyedStreamOps[T, K] extends DataStreamOps[T] {
  def stream: KeyedStream[T, K]

  /** Applies the given [[KeyedProcessFunction]] on the input stream, thereby creating a transformed output stream.
    *
    * The function will be called for every element in the stream and can produce zero or more output. The function can
    * also query the time and set timers. When reacting to the firing of set timers the function can emit yet more
    * elements.
    *
    * The function will be called for every element in the input streams and can produce zero or more output elements.
    * Contrary to the [[DataStream#flatMap(FlatMapFunction)]] function, this function can also query the time and set
    * timers. When reacting to the firing of set timers the function can directly emit elements and/or register yet more
    * timers.
    *
    * @param keyedProcessFunction
    *   The [[KeyedProcessFunction]] that is called for each element in the stream.
    */
  def process[R: TypeInformation](keyedProcessFunction: KeyedProcessFunction[K, T, R]): DataStream[R] = {
    stream.process(keyedProcessFunction, implicitly[TypeInformation[R]])
  }

  /** Creates a new [[DataStream]] by reducing the elements of this DataStream using an associative reduce function. An
    * independent aggregate is kept per key.
    */
  def reduce(fun: (T, T) => T): DataStream[T] = {
    val cleanFun = ClosureCleaner.clean(fun)
    val reducer = new ReduceFunction[T] {
      def reduce(v1: T, v2: T): T = { cleanFun(v1, v2) }
    }
    stream.reduce(reducer)
  }
  def reduceWith(fun: (T, T) => T): DataStream[T] = reduce(fun)

}
