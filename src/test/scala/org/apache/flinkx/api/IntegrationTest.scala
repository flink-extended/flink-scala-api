package org.apache.flinkx.api

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfterEach, Suite}

trait IntegrationTest extends BeforeAndAfterEach {
  this: Suite =>

  // It is recommended to always test your pipelines locally with a parallelism > 1 to identify bugs which only
  // surface for the pipelines executed in parallel. Setting number of slots per task manager and number of task
  // managers to 2 and parallelism to a multiple of them is a good starting point.
  val numberSlotsPerTaskManager = 2
  val numberTaskManagers        = 2
  val parallelism: Int          = numberSlotsPerTaskManager * numberTaskManagers

  val cluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(numberSlotsPerTaskManager)
      .setNumberTaskManagers(numberTaskManagers)
      .build()
  )

  val env: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.enableCheckpointing(1000)
    env.setRestartStrategy(RestartStrategies.noRestart())
    env.getConfig.disableGenericTypes()
    env
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cluster.before()
    IntegrationTestSink.values.clear()
  }

  override def afterEach(): Unit = {
    cluster.after()
    super.afterEach()
  }

  implicit final class DataStreamOps[T](private val dataStream: DataStream[T]) {
    import scala.collection.JavaConverters._

    def collect(): List[T] = {
      dataStream.addSink(new IntegrationTestSink[T])
      env.execute()
      IntegrationTestSink.values.asScala.toList.map(_.asInstanceOf[T])
    }
  }
}
