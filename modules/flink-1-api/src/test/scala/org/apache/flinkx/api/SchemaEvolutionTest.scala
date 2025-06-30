package org.apache.flinkx.api

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flinkx.api.SchemaEvolutionTest.{Click, ClickEvent, Event}
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Path}

class SchemaEvolutionTest extends AnyFlatSpec with Matchers {
  private implicit val eventTypeInfo: TypeInformation[Event]       = deriveTypeInformation[Event]
  private val clicks                                               =
    List(ClickEvent("a", "2021-01-01"), ClickEvent("b", "2021-01-01"), ClickEvent("c", "2021-01-01"))

  def createSerializer[T: TypeInformation] =
    implicitly[TypeInformation[T]].createSerializer(new ExecutionConfig())

  ignore should "generate blob for event=click+purchase" in {
    val buffer          = new ByteArrayOutputStream()
    val eventSerializer = createSerializer[Event]
    eventSerializer.serialize(Click("p1", clicks), new DataOutputViewStreamWrapper(buffer))
    Files.write(Path.of("src/test/resources/click.dat"), buffer.toByteArray)
  }

  it should "decode click when we added view" in {
    val buffer = this.getClass.getResourceAsStream("/click.dat")
    val click  = createSerializer[Event].deserialize(new DataInputViewStreamWrapper(buffer))
    click shouldBe Click("p1", clicks)
  }

}

object SchemaEvolutionTest {
  sealed trait Event
  case class Click(
      id: String,
      inFileClicks: List[ClickEvent],
      fieldInFile: String = "test1",
      fieldNotInFile: String = "test2"
  ) extends Event
  case class Purchase(price: Double) extends Event
  case class View(ts: Long)          extends Event
  case class ClickEvent(sessionId: String, date: String)
}
