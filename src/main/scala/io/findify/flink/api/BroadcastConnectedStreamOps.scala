package io.findify.flink.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{BroadcastConnectedStream, DataStream}
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}

trait BroadcastConnectedStreamOps[IN1, IN2] {
  def stream: BroadcastConnectedStream[IN1, IN2]

  /** Assumes as inputs a [[org.apache.flink.streaming.api.datastream.BroadcastStream]] and a [[KeyedStream]] and
    * applies the given [[KeyedBroadcastProcessFunction]] on them, thereby creating a transformed output stream.
    *
    * @param function
    *   The [[KeyedBroadcastProcessFunction]] applied to each element in the stream.
    * @tparam KS
    *   The type of the keys in the keyed stream.
    * @tparam OUT
    *   The type of the output elements.
    * @return
    *   The transformed [[DataStream]].
    */
  def process[KS, OUT: TypeInformation](function: KeyedBroadcastProcessFunction[KS, IN1, IN2, OUT]): DataStream[OUT] = {
    val outputTypeInfo: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]
    stream.process(function, outputTypeInfo)
  }

  /** Assumes as inputs a [[org.apache.flink.streaming.api.datastream.BroadcastStream]] and a non-keyed [[DataStream]]
    * and applies the given [[org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction]] on them, thereby
    * creating a transformed output stream.
    *
    * @param function
    *   The [[BroadcastProcessFunction]] applied to each element in the stream.
    * @tparam OUT
    *   The type of the output elements.
    * @return
    *   The transformed { @link DataStream}.
    */
  def process[OUT: TypeInformation](function: BroadcastProcessFunction[IN1, IN2, OUT]): DataStream[OUT] = {
    val outputTypeInfo: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]
    stream.process(function, outputTypeInfo)
  }
}
