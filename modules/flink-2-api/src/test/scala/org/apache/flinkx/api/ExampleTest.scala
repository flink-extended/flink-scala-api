package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.semiauto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class ExampleTest extends AnyFlatSpec with Matchers with IntegrationTest {
  import ExampleTest._

  it should "serialize elements with type mappers across operators" in {
    val result = env
      .fromElements(
        Event.View(UUID.randomUUID()),
        Event.Click("1"),
        Event.Purchase(1.0)
      )
      .keyBy(_.hashCode)
      .collect()

    result should have size 3
  }
}

object ExampleTest {
  sealed trait Event extends Product with Serializable

  object Event {
    final case class View(id: UUID)              extends Event
    final case class Click(id: String)           extends Event
    final case class Purchase(price: BigDecimal) extends Event

    implicit val eventTypeInfo: TypeInformation[Event] = deriveTypeInformation[Event]
  }
}
