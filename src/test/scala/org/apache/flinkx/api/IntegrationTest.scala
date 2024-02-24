package org.apache.flinkx.api

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfterEach, Suite}

trait IntegrationTest extends BeforeAndAfterEach {
  this: Suite =>

  lazy val cluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(1).setNumberTaskManagers(1).build()
  )

  lazy val env: StreamExecutionEnvironment = {
    cluster.getTestEnvironment.setAsContext()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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
