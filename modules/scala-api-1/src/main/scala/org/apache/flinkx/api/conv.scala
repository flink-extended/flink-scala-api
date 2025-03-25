package org.apache.flinkx.api

import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}

import scala.language.implicitConversions

object conv {
  implicit def toJavaEnv(e: StreamExecutionEnvironment): JavaEnv = e.getJavaEnv

  implicit def toJavaDataStream[T](d: DataStream[T]): JavaStream[T] = d.javaStream
}
