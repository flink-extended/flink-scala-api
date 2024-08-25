package org.apache.flinkx.api

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.mocks.MockSource
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

  // --------------------------------------------------------------------------
  //  mocks
  // --------------------------------------------------------------------------

  private class MockTypeInfo extends GenericTypeInfo[Integer](classOf[Integer]) {}
}
