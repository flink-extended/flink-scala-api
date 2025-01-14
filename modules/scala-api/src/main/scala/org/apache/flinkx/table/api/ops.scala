package org.apache.flinkx.table.api

import org.apache.flinkx.api.DataStream

import org.apache.flink.annotation.Experimental
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.datastream.{DataStream => JavaDataStream}

@Experimental
object ops {
  implicit class StreamTableEnvironmentOps(tEnv: StreamTableEnvironment) {
    def toStream(table: Table): DataStream[Row] =
      new DataStream(tEnv.toDataStream(table))
  }
}
