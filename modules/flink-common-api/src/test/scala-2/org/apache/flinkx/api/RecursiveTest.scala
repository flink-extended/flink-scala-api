package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.RecursiveTest.Node
import org.apache.flinkx.api.auto._
import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RecursiveTest extends AnyFlatSpec with Matchers with Inspectors with TestUtils {

  it should "fail fast with a comprehensive message when the derivation is recursive" in {
    val exception = intercept[FlinkRuntimeException](implicitly[TypeInformation[Node]])
    exception.getMessage shouldBe "Unsupported: recursivity detected in 'org.apache.flinkx.api.RecursiveTest.Node'."
  }

}

object RecursiveTest {

  case class Node(left: Option[Node], right: Option[Node])

}
