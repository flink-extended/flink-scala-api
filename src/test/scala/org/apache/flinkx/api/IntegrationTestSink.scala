package org.apache.flinkx.api

import org.apache.flink.streaming.api.functions.sink.SinkFunction

class IntegrationTestSink[T] extends SinkFunction[T] {
  override def invoke(value: T, context: SinkFunction.Context): Unit = IntegrationTestSink.values.add(value)
}

object IntegrationTestSink {
  val values: java.util.List[Any] = java.util.Collections.synchronizedList(new java.util.ArrayList())
}
