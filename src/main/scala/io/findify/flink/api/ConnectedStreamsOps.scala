package io.findify.flink.api

import io.findify.flink.ClosureCleaner
import io.findify.flink.api.ConnectedStreamsOps.JavaKeySelector
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.{ConnectedStreams, DataStream}
import org.apache.flink.streaming.api.functions.co.{
  CoFlatMapFunction,
  CoMapFunction,
  CoProcessFunction,
  KeyedCoProcessFunction
}
import org.apache.flink.util.Collector

trait ConnectedStreamsOps[IN1, IN2] {
  def stream: ConnectedStreams[IN1, IN2]

  /** Applies a CoMap transformation on the connected streams.
    *
    * The transformation consists of two separate functions, where the first one is called for each element of the first
    * connected stream, and the second one is called for each element of the second connected stream.
    *
    * @param fun1
    *   Function called per element of the first input.
    * @param fun2
    *   Function called per element of the second input.
    * @return
    *   The resulting data stream.
    */
  def map[R: TypeInformation](fun1: IN1 => R, fun2: IN2 => R): DataStream[R] = {
    val cleanFun1 = ClosureCleaner.clean(fun1)
    val cleanFun2 = ClosureCleaner.clean(fun2)
    val comapper = new CoMapFunction[IN1, IN2, R] {
      def map1(in1: IN1): R = cleanFun1(in1)
      def map2(in2: IN2): R = cleanFun2(in2)
    }

    stream.map(comapper)
  }

  /** Applies a CoMap transformation on these connected streams.
    *
    * The transformation calls [[CoMapFunction#map1]] for each element in the first stream and [[CoMapFunction#map2]]
    * for each element of the second stream.
    *
    * On can pass a subclass of [[org.apache.flink.streaming.api.functions.co.RichCoMapFunction]] to gain access to the
    * [[org.apache.flink.api.common.functions.RuntimeContext]] and to additional life cycle methods.
    *
    * @param coMapper
    *   The CoMapFunction used to transform the two connected streams
    * @return
    *   The resulting data stream
    */
  def map[R: TypeInformation](coMapper: CoMapFunction[IN1, IN2, R]): DataStream[R] = {
    val outType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.map(coMapper, outType)
  }

  /** Applies the given [[CoProcessFunction]] on the connected input streams, thereby creating a transformed output
    * stream.
    *
    * The function will be called for every element in the input streams and can produce zero or more output elements.
    * Contrary to the [[flatMap(CoFlatMapFunction)]] function, this function can also query the time and set timers.
    * When reacting to the firing of set timers the function can directly emit elements and/or register yet more timers.
    *
    * @param coProcessFunction
    *   The [[CoProcessFunction]] that is called for each element in the stream.
    * @return
    *   The transformed [[DataStream]].
    */
  def process[R: TypeInformation](coProcessFunction: CoProcessFunction[IN1, IN2, R]): DataStream[R] = {
    val outType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.process(coProcessFunction, outType)
  }

  /** Applies the given [[KeyedCoProcessFunction]] on the connected input keyed streams, thereby creating a transformed
    * output stream.
    *
    * The function will be called for every element in the input keyed streams and can produce zero or more output
    * elements. Contrary to the [[flatMap(CoFlatMapFunction)]] function, this function can also query the time and set
    * timers. When reacting to the firing of set timers the function can directly emit elements and/or register yet more
    * timers.
    *
    * @param keyedCoProcessFunction
    *   The [[KeyedCoProcessFunction]] that is called for each element in the stream.
    * @return
    *   The transformed [[DataStream]].
    */
  def process[K, R: TypeInformation](keyedCoProcessFunction: KeyedCoProcessFunction[K, IN1, IN2, R]): DataStream[R] = {

    val outType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.process(keyedCoProcessFunction, outType)
  }

  /** Applies a CoFlatMap transformation on these connected streams.
    *
    * The transformation calls [[CoFlatMapFunction#flatMap1]] for each element in the first stream and
    * [[CoFlatMapFunction#flatMap2]] for each element of the second stream.
    *
    * On can pass a subclass of [[org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction]] to gain access to
    * the [[org.apache.flink.api.common.functions.RuntimeContext]] and to additional life cycle methods.
    *
    * @param coFlatMapper
    *   The CoFlatMapFunction used to transform the two connected streams
    * @return
    *   The resulting data stream.
    */
  def flatMap[R: TypeInformation](coFlatMapper: CoFlatMapFunction[IN1, IN2, R]): DataStream[R] = {
    val outType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.flatMap(coFlatMapper, outType)
  }

  /** Applies a CoFlatMap transformation on the connected streams.
    *
    * The transformation consists of two separate functions, where the first one is called for each element of the first
    * connected stream, and the second one is called for each element of the second connected stream.
    *
    * @param fun1
    *   Function called per element of the first input.
    * @param fun2
    *   Function called per element of the second input.
    * @return
    *   The resulting data stream.
    */
  def flatMap[R: TypeInformation](
      fun1: (IN1, Collector[R]) => Unit,
      fun2: (IN2, Collector[R]) => Unit
  ): DataStream[R] = {
    val cleanFun1 = ClosureCleaner.clean(fun1)
    val cleanFun2 = ClosureCleaner.clean(fun2)
    val flatMapper = new CoFlatMapFunction[IN1, IN2, R] {
      def flatMap1(value: IN1, out: Collector[R]): Unit = cleanFun1(value, out)
      def flatMap2(value: IN2, out: Collector[R]): Unit = cleanFun2(value, out)
    }
    flatMap(flatMapper)
  }

  /** Applies a CoFlatMap transformation on the connected streams.
    *
    * The transformation consists of two separate functions, where the first one is called for each element of the first
    * connected stream, and the second one is called for each element of the second connected stream.
    *
    * @param fun1
    *   Function called per element of the first input.
    * @param fun2
    *   Function called per element of the second input.
    * @return
    *   The resulting data stream.
    */
  def flatMap[R: TypeInformation](fun1: IN1 => IterableOnce[R], fun2: IN2 => IterableOnce[R]): DataStream[R] = {
    val cleanFun1 = ClosureCleaner.clean(fun1)
    val cleanFun2 = ClosureCleaner.clean(fun2)

    val flatMapper = new CoFlatMapFunction[IN1, IN2, R] {
      def flatMap1(value: IN1, out: Collector[R]) = { cleanFun1(value).iterator.foreach(out.collect) }
      def flatMap2(value: IN2, out: Collector[R]) = { cleanFun2(value).iterator.foreach(out.collect) }
    }

    flatMap(flatMapper)
  }

  /** Keys the two connected streams together. After this operation, all elements with the same key from both streams
    * will be sent to the same parallel instance of the transformation functions.
    *
    * @param fun1
    *   The first stream's key function
    * @param fun2
    *   The second stream's key function
    * @return
    *   The key-grouped connected streams
    */
  def keyBy[KEY: TypeInformation](fun1: IN1 => KEY, fun2: IN2 => KEY): ConnectedStreams[IN1, IN2] = {

    val keyType = implicitly[TypeInformation[KEY]]

    val cleanFun1 = ClosureCleaner.clean(fun1)
    val cleanFun2 = ClosureCleaner.clean(fun2)

    val keyExtractor1 = new JavaKeySelector[IN1, KEY](cleanFun1)
    val keyExtractor2 = new JavaKeySelector[IN2, KEY](cleanFun2)

    stream.keyBy(keyExtractor1, keyExtractor2, keyType)
  }

}

object ConnectedStreamsOps {
  class JavaKeySelector[IN, K](private[this] val fun: IN => K) extends KeySelector[IN, K] {
    override def getKey(value: IN): K = fun(value)
  }
}
