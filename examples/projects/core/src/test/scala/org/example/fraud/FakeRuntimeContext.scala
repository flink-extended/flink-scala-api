package org.example.fraud

import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.api.common.accumulators.{
  Accumulator,
  DoubleCounter,
  Histogram,
  IntCounter,
  LongCounter
}
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.api.common.externalresource.ExternalResourceInfo
import org.apache.flink.api.common.functions.{
  BroadcastVariableInitializer,
  RuntimeContext
}
import org.apache.flink.api.common.state.{
  AggregatingState,
  AggregatingStateDescriptor,
  ListState,
  ListStateDescriptor,
  MapState,
  MapStateDescriptor,
  ReducingState,
  ReducingStateDescriptor,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.metrics.groups.OperatorMetricGroup

import java.util

class FakeRuntimeContext extends RuntimeContext:

  override def getJobId: JobID = ???

  override def getTaskName: String = ???

  override def getMetricGroup: OperatorMetricGroup = ???

  override def getNumberOfParallelSubtasks: Int = ???

  override def getMaxNumberOfParallelSubtasks: Int = ???

  override def getIndexOfThisSubtask: Int = ???

  override def getAttemptNumber: Int = ???

  override def getTaskNameWithSubtasks: String = ???

  override def getExecutionConfig: ExecutionConfig = ???

  override def getUserCodeClassLoader: ClassLoader = ???

  override def registerUserCodeClassLoaderReleaseHookIfAbsent(
      releaseHookName: String,
      releaseHook: Runnable
  ): Unit = ???

  override def addAccumulator[V, A <: Serializable](
      name: String,
      accumulator: Accumulator[V, A]
  ): Unit = ???

  override def getAccumulator[V, A <: Serializable](
      name: String
  ): Accumulator[V, A] = ???

  override def getIntCounter(name: String): IntCounter = ???

  override def getLongCounter(name: String): LongCounter = ???

  override def getDoubleCounter(name: String): DoubleCounter = ???

  override def getHistogram(name: String): Histogram = ???

  override def getExternalResourceInfos(
      resourceName: String
  ): util.Set[ExternalResourceInfo] = ???

  override def hasBroadcastVariable(name: String): Boolean = ???

  override def getBroadcastVariable[RT](name: String): util.List[RT] = ???

  override def getBroadcastVariableWithInitializer[T, C](
      name: String,
      initializer: BroadcastVariableInitializer[T, C]
  ): C = ???

  override def getDistributedCache: DistributedCache = ???

  override def getListState[T](
      stateProperties: ListStateDescriptor[T]
  ): ListState[T] = ???

  override def getReducingState[T](
      stateProperties: ReducingStateDescriptor[T]
  ): ReducingState[T] = ???

  override def getAggregatingState[IN, ACC, OUT](
      stateProperties: AggregatingStateDescriptor[IN, ACC, OUT]
  ): AggregatingState[IN, OUT] = ???

  override def getMapState[UK, UV](
      stateProperties: MapStateDescriptor[UK, UV]
  ): MapState[UK, UV] = ???

  override def getState[T](
      stateProperties: ValueStateDescriptor[T]
  ): ValueState[T] =
    new ValueState[T] {
      var v: T = null.asInstanceOf[T]
      override def clear(): Unit = v = null.asInstanceOf[T]
      override def update(value: T): Unit = v = value
      override def value(): T = v
    }
