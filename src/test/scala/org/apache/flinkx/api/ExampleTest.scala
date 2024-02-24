package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExampleTest extends AnyFlatSpec with Matchers with IntegrationTest {
  import ExampleTest._

  it should "run example code" in {
    val result = env
      .fromCollection(List(Event.Click("1"), Event.Purchase(1.0)))
      .collect()

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
}
