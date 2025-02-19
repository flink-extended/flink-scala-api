package org.apache.flinkx.api

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.mocks.MockSource
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.util.FlinkRuntimeException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success, Try}

class StreamExecutionEnvironmentTest extends AnyFlatSpec with Matchers with IntegrationTest {

  it should "create a stream from a source" in {
    implicit val typeInfo: TypeInformation[Integer] = new MockTypeInfo()

    val stream = env.fromSource(
      new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 1),
      WatermarkStrategy.noWatermarks(),
      "test source"
    )

    stream.dataType shouldBe typeInfo
  }

  it should "create a stream from a sequence" in {
    import org.apache.flinkx.api.serializers._
    val typeInfo = implicitly[TypeInformation[Long]]

    val stream = env.fromSequence(1, 100)

    stream.dataType shouldBe typeInfo
  }

  "From Flink 1.19, TypeInformation.of(Class)" should "fail-fast trying to resolve Scala type" in {
    Try(Class.forName("org.apache.flink.configuration.PipelineOptions").getField("SERIALIZATION_CONFIG")) match {
      case Failure(_) => // Before Flink 1.19: no fail-fast, exception happens at execution
        implicit val typeInfo: TypeInformation[Option[Int]] = TypeInformation.of(classOf[Option[Int]])
        val stream                                          = env.fromElements(Some(1), None, Some(100))
        val exception = intercept[UnsupportedOperationException] {
          stream.executeAndCollect(3)
        }
        exception.getMessage should startWith(
          "Generic types have been disabled in the ExecutionConfig and type scala.Option is treated as a generic type."
        )

      case Success(_) => // From Flink 1.19: fail-fast at Scala type resolution
        val exception = intercept[FlinkRuntimeException] {
          TypeInformation.of(classOf[Option[Int]])
        }
        exception.getMessage should startWith("You are using a 'Class' to resolve 'scala.Option' Scala type.")
    }
  }

  // --------------------------------------------------------------------------
  //  mocks
  // --------------------------------------------------------------------------

  private class MockTypeInfo extends GenericTypeInfo[Integer](classOf[Integer]) {}
}
