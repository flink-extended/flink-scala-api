package io.findify.flink.api

import io.findify.flink.ClosureCleaner
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.io.{FileInputFormat, InputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.{FileProcessingMode, SourceFunction}
import org.apache.flink.util.SplittableIterator
import scala.jdk.CollectionConverters._

trait StreamExecutionEnvironmentOps {
  def env: StreamExecutionEnvironment

  /** Creates a DataStream that contains the given elements. The elements must all be of the same type.
    *
    * Note that this operation will result in a non-parallel data source, i.e. a data source with a parallelism of one.
    */
  def fromElements[T: TypeInformation](data: T*): DataStream[T] = {
    fromCollection(data)
  }

  /** Creates a DataStream from the given non-empty [[Seq]]. The elements need to be serializable because the framework
    * may move the elements into the cluster if needed.
    *
    * Note that this operation will result in a non-parallel data source, i.e. a data source with a parallelism of one.
    */
  def fromCollection[T: TypeInformation](data: Seq[T]): DataStream[T] = {
    val typeInfo   = implicitly[TypeInformation[T]]
    val collection = data.asJavaCollection
    env.fromCollection(collection, typeInfo)
  }

  /** Creates a DataStream from the given [[Iterator]].
    *
    * Note that this operation will result in a non-parallel data source, i.e. a data source with a parallelism of one.
    */
  def fromCollection[T: TypeInformation](data: Iterator[T]): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    env.fromCollection(data.asJava, typeInfo)
  }

  /** Creates a DataStream from the given [[SplittableIterator]].
    */
  def fromParallelCollection[T: TypeInformation](data: SplittableIterator[T]): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    env.fromParallelCollection(data, typeInfo)
  }

  /** Reads the contents of the user-specified path based on the given [[FileInputFormat]]. Depending on the provided
    * [[FileProcessingMode]], the source may periodically monitor (every `interval` ms) the path for new data
    * ([[FileProcessingMode.PROCESS_CONTINUOUSLY]]), or process once the data currently in the path and exit
    * ([[FileProcessingMode.PROCESS_ONCE]]). In addition, if the path contains files not to be processed, the user can
    * specify a custom [[FilePathFilter]]. As a default implementation you can use
    * [[FilePathFilter.createDefaultFilter()]].
    *
    * ** NOTES ON CHECKPOINTING: ** If the `watchType` is set to [[FileProcessingMode#PROCESS_ONCE]], the source
    * monitors the path ** once **, creates the [[org.apache.flink.core.fs.FileInputSplit FileInputSplits]] to be
    * processed, forwards them to the downstream [[ContinuousFileReaderOperator readers]] to read the actual data, and
    * exits, without waiting for the readers to finish reading. This implies that no more checkpoint barriers are going
    * to be forwarded after the source exits, thus having no checkpoints after that point.
    *
    * @param inputFormat
    *   The input format used to create the data stream
    * @param filePath
    *   The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
    * @param watchType
    *   The mode in which the source should operate, i.e. monitor path and react to new data, or process once and exit
    * @param interval
    *   In the case of periodic path monitoring, this specifies the interval (in millis) between consecutive path scans
    * @return
    *   The data stream that represents the data read from the given file
    */
  def readFile[T: TypeInformation](
      inputFormat: FileInputFormat[T],
      filePath: String,
      watchType: FileProcessingMode,
      interval: Long
  ): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    env.readFile(inputFormat, filePath, watchType, interval, typeInfo)
  }

  /** Generic method to create an input data stream with a specific input format. Since all data streams need specific
    * information about their types, this method needs to determine the type of the data produced by the input format.
    * It will attempt to determine the data type by reflection, unless the input format implements the
    * ResultTypeQueryable interface.
    */
  def createInput[T: TypeInformation](inputFormat: InputFormat[T, _]): DataStream[T] =
    if (inputFormat.isInstanceOf[ResultTypeQueryable[_]]) {
      env.createInput(inputFormat)
    } else {
      env.createInput(inputFormat, implicitly[TypeInformation[T]])
    }

  /** Create a DataStream using a user defined source function for arbitrary source functionality. By default sources
    * have a parallelism of 1. To enable parallel execution, the user defined source should implement
    * ParallelSourceFunction or extend RichParallelSourceFunction. In these cases the resulting source will have the
    * parallelism of the environment. To change this afterwards call DataStreamSource.setParallelism(int)
    */
  def addSource[T: TypeInformation](function: SourceFunction[T]): DataStream[T] = {
    val cleanFun = ClosureCleaner.clean(function)
    val typeInfo = implicitly[TypeInformation[T]]
    env.addSource(cleanFun, typeInfo)
  }

  /** Create a DataStream using a user defined source function for arbitrary source functionality.
    */
  def addSource[T: TypeInformation](function: SourceContext[T] => Unit): DataStream[T] = {
    val sourceFunction = new SourceFunction[T] {
      val cleanFun = ClosureCleaner.clean(function)
      override def run(ctx: SourceContext[T]) {
        cleanFun(ctx)
      }
      override def cancel() = {}
    }
    addSource(sourceFunction)
  }

  /** Create a DataStream using a [[Source]].
    */
  def fromSource[T: TypeInformation](
      source: Source[T, _ <: SourceSplit, _],
      watermarkStrategy: WatermarkStrategy[T],
      sourceName: String
  ): DataStream[T] = {

    val typeInfo = implicitly[TypeInformation[T]]
    env.fromSource(source, watermarkStrategy, sourceName, typeInfo)
  }
}
