package io.findify.flink.api

import io.findify.flink.ClosureCleaner
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction, Partitioner}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{ConnectedStreams, DataStream, DataStreamSink, KeyedStream}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.util.Collector

trait DataStreamOps[T] {
  def stream: DataStream[T]

  /** Groups the elements of a DataStream by the given K key to be used with grouped operators like grouped reduce or
    * grouped aggregations.
    */
  def keyBy[K: TypeInformation](fun: T => K): KeyedStream[T, K] = {
    val cleanFun                    = ClosureCleaner.clean(fun)
    val keyType: TypeInformation[K] = implicitly[TypeInformation[K]]

    val keyExtractor = new KeySelector[T, K] with ResultTypeQueryable[K] {
      def getKey(in: T)                                = cleanFun(in)
      override def getProducedType: TypeInformation[K] = keyType
    }
    new KeyedStream[T, K](stream, keyExtractor, keyType)
  }

  /** Groups the elements of a DataStream by the given K key to be used with grouped operators like grouped reduce or
    * grouped aggregations.
    */
  def keyBy[K: TypeInformation](fun: KeySelector[T, K]): KeyedStream[T, K] = {
    val cleanFun                    = ClosureCleaner.clean(fun)
    val keyType: TypeInformation[K] = implicitly[TypeInformation[K]]
    new KeyedStream[T, K](stream, cleanFun, keyType)
  }

  def keyingBy[K: TypeInformation](fun: T => K): KeyedStream[T, K] = keyBy(fun)

  /** Partitions a DataStream on the key returned by the selector, using a custom partitioner. This method takes the key
    * selector to get the key to partition on, and a partitioner that accepts the key type.
    *
    * Note: This method works only on single field keys, i.e. the selector cannot return tuples of fields.
    */
  def partitionCustom[K: TypeInformation](partitioner: Partitioner[K], fun: T => K): DataStream[T] = {
    val keyType  = implicitly[TypeInformation[K]]
    val cleanFun = ClosureCleaner.clean(fun)
    val keyExtractor = new KeySelector[T, K] with ResultTypeQueryable[K] {
      def getKey(in: T)                                  = cleanFun(in)
      override def getProducedType(): TypeInformation[K] = keyType
    }
    stream.partitionCustom(partitioner, keyExtractor)
  }

  /** Initiates an iterative part of the program that creates a loop by feeding back data streams. To create a streaming
    * iteration the user needs to define a transformation that creates two DataStreams. The first one is the output that
    * will be fed back to the start of the iteration and the second is the output stream of the iterative part.
    *
    * stepfunction: initialStream => (feedback, output)
    *
    * A common pattern is to use output splitting to create feedback and output DataStream. Please see the side outputs
    * of [[ProcessFunction]] method of the DataStream
    *
    * By default a DataStream with iteration will never terminate, but the user can use the maxWaitTime parameter to set
    * a max waiting time for the iteration head. If no data received in the set time the stream terminates.
    *
    * Parallelism of the feedback stream must match the parallelism of the original stream. Please refer to the
    * [[setParallelism]] method for parallelism modification
    */
  def iterate[R, F: TypeInformation](
      stepFunction: ConnectedStreams[T, F] => (DataStream[F], DataStream[R]),
      maxWaitTimeMillis: Long
  ): DataStream[R] = {
    val feedbackType: TypeInformation[F] = implicitly[TypeInformation[F]]

    val connectedIterativeStream = stream.iterate(maxWaitTimeMillis).withFeedbackType(feedbackType)

    val (feedback, output) = stepFunction(connectedIterativeStream)
    connectedIterativeStream.closeWith(feedback)
    output
  }

  /** Creates a new DataStream by applying the given function to every element of this DataStream.
    */
  def map[R: TypeInformation](fun: T => R): DataStream[R] = {
    val cleanFun = ClosureCleaner.clean(fun)
    val mapper = new MapFunction[T, R] {
      def map(in: T): R = cleanFun(in)
    }

    map(mapper)
  }

  def mapWith[R: TypeInformation](fun: T => R): DataStream[R] = map(fun)

  /** Creates a new DataStream by applying the given function to every element of this DataStream.
    */
  def map[R: TypeInformation](mapper: MapFunction[T, R]): DataStream[R] = {
    val outType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.map(mapper, outType)
  }

  /** Creates a new DataStream by applying the given function to every element and flattening the results.
    */
  def flatMap[R: TypeInformation](flatMapper: FlatMapFunction[T, R]): DataStream[R] = {
    val outType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.flatMap(flatMapper, outType)
  }

  /** Creates a new DataStream by applying the given function to every element and flattening the results.
    */
  def flatMap[R: TypeInformation](fun: (T, Collector[R]) => Unit): DataStream[R] = {
    val cleanFun = ClosureCleaner.clean(fun)
    val flatMapper = new FlatMapFunction[T, R] {
      def flatMap(in: T, out: Collector[R]) { cleanFun(in, out) }
    }
    flatMap(flatMapper)
  }

  /** Creates a new DataStream by applying the given function to every element and flattening the results.
    */
  def flatMap[R: TypeInformation](fun: T => IterableOnce[R]): DataStream[R] = {
    val cleanFun = ClosureCleaner.clean(fun)
    val flatMapper = new FlatMapFunction[T, R] {
      def flatMap(in: T, out: Collector[R]) { cleanFun(in).iterator.foreach(out.collect) }
    }
    flatMap(flatMapper)
  }
  def flatMapWith[R: TypeInformation](fun: T => IterableOnce[R]): DataStream[R] = flatMap(fun)

  /** Applies the given [[ProcessFunction]] on the input stream, thereby creating a transformed output stream.
    *
    * The function will be called for every element in the stream and can produce zero or more output.
    *
    * @param processFunction
    *   The [[ProcessFunction]] that is called for each element in the stream.
    */
  def process[R: TypeInformation](processFunction: ProcessFunction[T, R]): DataStream[R] = {
    val outType: TypeInformation[R] = implicitly[TypeInformation[R]]
    stream.process(processFunction, outType)
  }

  /** Creates a new DataStream that contains only the elements satisfying the given filter predicate.
    */
  def filter(fun: T => Boolean): DataStream[T] = {
    val cleanFun = ClosureCleaner.clean(fun)
    val filterFun = new FilterFunction[T] {
      def filter(in: T) = cleanFun(in)
    }
    stream.filter(filterFun)
  }

  def filterWith(fun: T => Boolean): DataStream[T] = filter(fun)

  /** Assigns timestamps to the elements in the data stream and generates watermarks to signal event time progress. The
    * given [[WatermarkStrategy is used to create a [[TimestampAssigner]] and
    * [[org.apache.flink.api.common.eventtime.WatermarkGenerator]].
    *
    * For each event in the data stream, the [[TimestampAssigner#extractTimestamp(Object, long)]] method is called to
    * assign an event timestamp.
    *
    * For each event in the data stream, the [[WatermarkGenerator#onEvent(Object, long, WatermarkOutput)]] will be
    * called.
    *
    * Periodically (defined by the [[ExecutionConfig#getAutoWatermarkInterval()]]), the
    * [[WatermarkGenerator#onPeriodicEmit(WatermarkOutput)]] method will be called.
    *
    * Common watermark generation patterns can be found as static methods in the
    * [[org.apache.flink.api.common.eventtime.WatermarkStrategy]] class.
    */
  def assignTimestampsAndWatermarks(watermarkStrategy: WatermarkStrategy[T]): DataStream[T] = {
    val cleanedStrategy = ClosureCleaner.clean(watermarkStrategy)
    stream.assignTimestampsAndWatermarks(cleanedStrategy)
  }

  /** Assigns timestamps to the elements in the data stream and periodically creates watermarks to signal event time
    * progress.
    *
    * This method is a shortcut for data streams where the element timestamp are known to be monotonously ascending
    * within each parallel stream. In that case, the system can generate watermarks automatically and perfectly by
    * tracking the ascending timestamps.
    *
    * For cases where the timestamps are not monotonously increasing, use the more general methods
    * [[assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks)]] and
    * [[assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks)]].
    */
  def assignAscendingTimestamps(extractor: T => Long): DataStream[T] = {
    val cleanExtractor = ClosureCleaner.clean(extractor)
    val extractorFunction = new AscendingTimestampExtractor[T] {
      def extractAscendingTimestamp(element: T): Long = {
        cleanExtractor(element)
      }
    }
    stream.assignTimestampsAndWatermarks(extractorFunction)
  }

  /** Adds the given sink to this DataStream. Only streams with sinks added will be executed once the
    * StreamExecutionEnvironment.execute(...) method is called.
    */
  def addSink(fun: T => Unit): DataStreamSink[T] = {
    val cleanFun = ClosureCleaner.clean(fun)
    val sinkFunction = new SinkFunction[T] {
      override def invoke(value: T, context: SinkFunction.Context): Unit = cleanFun(value)
    }
    stream.addSink(sinkFunction)
  }

}
