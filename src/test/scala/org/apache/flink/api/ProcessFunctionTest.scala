package org.apache.flink.api

import org.apache.flink.api.serializers._
import org.apache.flink.api.ProcessFunctionTest._
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.util._
import org.apache.flink.util.Collector
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ProcessFunctionTest extends AnyFlatSpec with Matchers {

  "ProcessFunction" should "return same values unchanged" in {
    val serializer = deriveTypeInformation[DataPackage].createSerializer(new ExecutionConfig())
    val harness    = prepareTestHarness(new SimpleFunction(), serializer)
    sources.foreach(data => harness.processElement(data, harness.getProcessingTime))
    val result = harness.extractOutputValues().asScala.toList
    result should contain theSameElementsAs sources
  }
}

object ProcessFunctionTest {

  case class DataPackage(id: String, ints: List[Int]) extends Serializable

  val sources: List[DataPackage] = List(
    DataPackage("3", List(3, 2, 1)),
    DataPackage("2", List(2)),
    DataPackage("1", Nil)
  )

  private def prepareTestHarness[D <: DataPackage, R <: DataPackage](
      processFunction: KeyedProcessFunction[String, D, R],
      serializer: TypeSerializer[R]
  ): KeyedOneInputStreamOperatorTestHarness[String, D, R] = {
    val harness = new KeyedOneInputStreamOperatorTestHarness[String, D, R](
      new KeyedProcessOperator[String, D, R](processFunction),
      (data: D) => data.id,
      Types.STRING,
      1,
      1,
      0
    )
    harness.setup(serializer)
    harness.open()
    harness
  }

  @SerialVersionUID(1)
  final class SimpleFunction extends KeyedProcessFunction[String, DataPackage, DataPackage] {

    type ContextT = KeyedProcessFunction[String, DataPackage, DataPackage]#Context

    def processElement(value: DataPackage, ctx: ContextT, out: Collector[DataPackage]): Unit = {
      out.collect(value)
    }
  }
}
