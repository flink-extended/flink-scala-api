package io.findify.flink

import org.apache.flink.streaming.api.datastream.{
  BroadcastConnectedStream,
  ConnectedStreams,
  DataStream,
  DataStreamSource,
  JoinedStreams,
  KeyedStream,
  WindowedStream
}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.windows.Window

package object api {
  implicit class ScalaDataStream[T](val stream: DataStream[T])             extends DataStreamOps[T]
  implicit class ScalaKeyedDataStream[T, K](val stream: KeyedStream[T, K]) extends KeyedStreamOps[T, K]
  implicit class ScalaDataStreamSource[T](val stream: DataStreamSource[T]) extends DataStreamOps[T]
  implicit class ScalaConnectedStream[IN1, IN2](val stream: ConnectedStreams[IN1, IN2])
      extends ConnectedStreamsOps[IN1, IN2]

  implicit class ScalaStreamExecutionEnvironment(val env: StreamExecutionEnvironment)
      extends StreamExecutionEnvironmentOps

  implicit class ScalaBroadcastConnectedStream[IN1, IN2](val stream: BroadcastConnectedStream[IN1, IN2])
      extends BroadcastConnectedStreamOps[IN1, IN2]

  implicit class ScalaJoinedStream[IN1, IN2](val stream: JoinedStreams[IN1, IN2]) extends JoinedStreamsOps[IN1, IN2]

  implicit class ScalaWindowedStream[T, K, W <: Window](val stream: WindowedStream[T, K, W])
      extends WindowedStreamOps[T, K, W]
}
