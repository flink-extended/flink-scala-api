package org.apache.flinkx.api

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType
import org.apache.flink.configuration.{Configuration, PipelineOptions, RestartStrategyOptions}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait IntegrationTest extends BeforeAndAfterEach with BeforeAndAfterAll {
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
    val config = new Configuration()
    config.set(RestartStrategyOptions.RESTART_STRATEGY, RestartStrategyType.NO_RESTART_STRATEGY.getMainValue)
    config.set(PipelineOptions.GENERIC_TYPES, java.lang.Boolean.FALSE)
    val env = StreamExecutionEnvironment.getExecutionEnvironment(config)
    env.setParallelism(parallelism)
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.enableCheckpointing(1000)
    env
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cluster.before()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    IntegrationTestSink.values.clear()
  }

  override def afterAll(): Unit = {
    cluster.after()
    super.afterAll()
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
