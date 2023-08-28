package org.apache.flinkx.api

import org.apache.flinkx.api.SchemaEvolutionTest.{Click, Event}
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaEvolutionTest extends AnyFlatSpec with Matchers {
  private val ti = deriveTypeInformation[Event]

//  it should "generate blob for event=click+purchase" in {
//    val buffer = new ByteArrayOutputStream()
//    eventSerializer.serialize(Click("p1"), new DataOutputViewStreamWrapper(buffer))
//    Files.write(buffer.toByteArray, new File("/tmp/out.dat"))
//  }

  it should "decode click when we added view" in {
    val buffer = this.getClass.getResourceAsStream("/click.dat")
    val click  = ti.createSerializer(null).deserialize(new DataInputViewStreamWrapper(buffer))
    click shouldBe Click("p1")
  }
}

object SchemaEvolutionTest {
  sealed trait Event
  case class Click(id: String)       extends Event
  case class Purchase(price: Double) extends Event
  case class View(ts: Long)          extends Event
}
