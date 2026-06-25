package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.RecursiveDerivationTest.RecursiveNode
import org.apache.flinkx.api.auto._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RecursiveDerivationTest extends AnyFunSuite with Matchers {

  test("Recursive derivation is not supported and fails with a StackOverflowError") {
    assertThrows[StackOverflowError](implicitly[TypeInformation[RecursiveNode]])
  }

}

object RecursiveDerivationTest {

  case class RecursiveNode(left: Option[RecursiveNode], right: Option[RecursiveNode])

}
