package org.apache.flink.api

import org.apache.flinkadt.api.typeinfo.CaseClassTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}
import org.apache.flink.streaming.api.datastream.{ConnectedStreams => ConnectedJavaStreams}
import org.apache.flink.streaming.api.datastream.{BroadcastConnectedStream => BroadcastConnectedJavaStreams}
import org.apache.flink.streaming.api.datastream.{KeyedStream => KeyedJavaStream}

import language.implicitConversions
import language.experimental.macros

object ScalaStreamOps {

  /** Converts an [[org.apache.flink.streaming.api.datastream.DataStream]] to a [[org.apache.flink.api.DataStream]].
    */
  def asScalaStream[R](stream: JavaStream[R]) = new DataStream[R](stream)

  /** Converts an [[org.apache.flink.streaming.api.datastream.KeyedStream]] to a [[org.apache.flink.api.KeyedStream]].
    */
  def asScalaStream[R, K](stream: KeyedJavaStream[R, K]) = new KeyedStream[R, K](stream)

  /** Converts an [[org.apache.flink.streaming.api.datastream.ConnectedStreams]] to a
    * [[org.apache.flink.api.ConnectedStreams]].
    */
  def asScalaStream[IN1, IN2](stream: ConnectedJavaStreams[IN1, IN2]) = new ConnectedStreams[IN1, IN2](stream)

  /** Converts an [[org.apache.flink.streaming.api.datastream.BroadcastConnectedStream]] to a
    * [[org.apache.flink.api.BroadcastConnectedStream]].
    */
  def asScalaStream[IN1, IN2](stream: BroadcastConnectedJavaStreams[IN1, IN2]) =
    new BroadcastConnectedStream[IN1, IN2](stream)

  private[flink] def fieldNames2Indices(typeInfo: TypeInformation[_], fields: Array[String]): Array[Int] = {    
    typeInfo match {
      case ti: CaseClassTypeInfo[_] =>
        val result = ti.getFieldIndices(fields)

        if (result.contains(-1)) {
          throw new IllegalArgumentException(
            "Fields '" + fields.mkString(", ") +
              "' are not valid for '" + ti.toString + "'."
          )
        }

        result

      case _ =>
        throw new UnsupportedOperationException(
          "Specifying fields by name is only" +
            "supported on Case Classes (for now)."
        )
    }
  }

}
