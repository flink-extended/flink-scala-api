package org.apache.flinkx.api

import org.apache.flink.annotation.Experimental
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.{DataStreamUtils => JavaStreamUtils}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import ScalaStreamOps._

/** This class provides simple utility methods for collecting a [[DataStream]], effectively enriching it with the
  * functionality encapsulated by [[DataStreamUtils]].
  *
  * This experimental class is relocated from flink-streaming-contrib.
  *
  * @param self
  *   DataStream
  */
@Experimental
class DataStreamUtils[T: TypeInformation: ClassTag](val self: DataStream[T]) {

  /** Returns a scala iterator to iterate over the elements of the DataStream.
    * @return
    *   The iterator
    *
    * @deprecated
    *   Replaced with [[DataStream#executeAndCollect]].
    */
  def collect(): Iterator[T] = {
    JavaStreamUtils.collect(self.javaStream).asScala
  }

  /** Reinterprets the given [[DataStream]] as a [[KeyedStream]], which extracts keys with the given [[KeySelector]].
    *
    * IMPORTANT: For every partition of the base stream, the keys of events in the base stream must be partitioned
    * exactly in the same way as if it was created through a [[DataStream#keyBy(KeySelector)]].
    *
    * @param keySelector
    *   Function that defines how keys are extracted from the data stream.
    * @return
    *   The reinterpretation of the [[DataStream]] as a [[KeyedStream]].
    */
  def reinterpretAsKeyedStream[K: TypeInformation](keySelector: T => K): KeyedStream[T, K] = {

    val keyTypeInfo     = implicitly[TypeInformation[K]]
    val cleanSelector   = clean(keySelector)
    val javaKeySelector = new JavaKeySelector[T, K](cleanSelector)

    asScalaStream(JavaStreamUtils.reinterpretAsKeyedStream(self.javaStream, javaKeySelector, keyTypeInfo))
  }

  private[flinkx] def clean[F <: AnyRef](f: F): F = {
    new StreamExecutionEnvironment(self.javaStream.getExecutionEnvironment).scalaClean(f)
  }
}
