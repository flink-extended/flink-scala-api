package org.apache.flinkx.table.api

import org.apache.flinkx.api.DataStream

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

implicit class ops(tEnv: StreamTableEnvironment) {
  def toStream(table: Table): DataStream[Row] =
    DataStream(tEnv.toDataStream(table))
}
