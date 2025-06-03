package org.apache.flinkx.api

import org.apache.flinkx.api.SchemaEvolutionTest.{Click, ClickEvent, Event, NoArityTest}
import org.apache.flinkx.api.serializers._
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.api.common.ExecutionConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Path}

class SchemaEvolutionTest extends AnyFlatSpec with Matchers {
  private implicit val eventTypeInfo: TypeInformation[Event]       = deriveTypeInformation[Event]
  private implicit val arityTestInfo: TypeInformation[NoArityTest] = deriveTypeInformation[NoArityTest]
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

  ignore should "generate blob for no arity test" in {
    val buffer          = new ByteArrayOutputStream()
    val eventSerializer = createSerializer[NoArityTest]
    eventSerializer.serialize(NoArityTest(4, 3, List("test")), new DataOutputViewStreamWrapper(buffer))
    Files.write(Path.of("src/test/resources/without-arity-test.dat"), buffer.toByteArray)
  }

  it should "decode class without arity info" in {
    val buffer     = this.getClass.getResourceAsStream("/without-arity-test.dat")
    val serializer = createSerializer[NoArityTest] match {
      case s: CaseClassSerializer[_] => s
      case s                         => fail(s"Derived serializer must be of CaseClassSerializer type, but was $s")
    }
    val decoded =
      serializer.deserializeFromSource(new DataInputViewStreamWrapper(buffer), classArityUsageDisabled = true)
    decoded shouldBe NoArityTest(4, 3, List("test"))
  }
}

object SchemaEvolutionTest {
  case class NoArityTest(field1: Long, field2: Long, field3: List[String] = Nil)

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
