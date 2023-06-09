package org.apache.flink.api

import org.apache.flink.api.serializers._
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn

class ExampleTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  import ExampleTest._

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

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cluster.before()
  }

  override def afterAll(): Unit = {
    cluster.after()
    super.afterAll()
  }

  it should "run example code" in {
    val result = env
      .fromScalaCollection(List(Event.Click("1"), Event.Purchase(1.0)))
      .executeAndCollect(10)

    result.size shouldBe 2
  }
}

object ExampleTest {
  sealed trait Event extends Product with Serializable

  object Event {
    final case class Click(id: String)       extends Event
    final case class Purchase(price: Double) extends Event

    implicit val eventTypeInfo: TypeInformation[Event] = deriveTypeInformation[Event]
  }

  implicit final class EnvOps(private val env: StreamExecutionEnvironment) extends AnyVal {
    import scala.collection.JavaConverters._

    @nowarn("cat=deprecation")
    def fromScalaCollection[A](data: Seq[A])(implicit typeInformation: TypeInformation[A]): DataStreamSource[A] =
      env.fromCollection(data.asJava, typeInformation)
  }
}
