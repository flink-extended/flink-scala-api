package org.apache.flink.api

import org.apache.flink.api._
import org.apache.flink.api.serializers._
import org.apache.flink.api.ProcessFunctionTest._
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.util._
import org.apache.flink.util.Collector
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ProcessFunctionTest extends AnyFlatSpec with Matchers {

  "ProcessFunction" should "return same values unchanged" in {
    val harness = prepareTestHarness(new SimpleFunction())
    sources.foreach(data => harness.processElement(data, harness.getProcessingTime))
    val result = harness.extractOutputValues().asScala.toList
    // println(result)
    result shouldBe sources
  }
}

object ProcessFunctionTest {

  case class DataPackage(id: String, ints: List[Int]) extends Serializable

  val sources: List[DataPackage] = List(
    DataPackage("3", List(3, 2, 1)),
    DataPackage("2", List(2))
//    DataPackage("1", Nil)
  )

  private def prepareTestHarness[D <: DataPackage, R <: DataPackage](
      processFunction: KeyedProcessFunction[String, D, R]
  ): KeyedOneInputStreamOperatorTestHarness[String, D, R] =
    ProcessFunctionTestHarnesses.forKeyedProcessFunction[String, D, R](
      processFunction,
      _.id,
      Types.STRING
    )

  @SerialVersionUID(1)
  final class SimpleFunction extends KeyedProcessFunction[String, DataPackage, DataPackage] {

    type ContextT = KeyedProcessFunction[String, DataPackage, DataPackage]#Context

    def processElement(value: DataPackage, ctx: ContextT, out: Collector[DataPackage]): Unit = {
      out.collect(value)
    }
  }
}
