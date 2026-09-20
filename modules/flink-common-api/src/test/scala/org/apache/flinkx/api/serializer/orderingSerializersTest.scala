package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.serializer.SnapshotTestUtil._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OptionOrderingSerializerTest extends AnyFlatSpec with Matchers {

  it should "be compatible as is when the serializer of the ordering is compatible" in {
    val registeredSnapshot = optionOrderingSerializer(DefaultStringOrderingSerializer).snapshotConfiguration()
    val restoredSnapshot   =
      savepointed[Ordering[Option[String]]](optionOrderingSerializer(DefaultStringOrderingSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when the serializer of the ordering is incompatible" in {
    val registeredSnapshot = optionOrderingSerializer(DefaultStringOrderingSerializer).snapshotConfiguration()
    val restoredSnapshot   =
      savepointed[Ordering[Option[String]]](optionOrderingSerializer(reversedStringOrderingSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  private def optionOrderingSerializer(orderingSerializer: TypeSerializer[Ordering[String]]) =
    new OptionOrderingSerializer[String](orderingSerializer)

  private def reversedStringOrderingSerializer = new ReverseOrderingSerializer[String](DefaultStringOrderingSerializer)

}

class ReverseOrderingSerializersTest extends AnyFlatSpec with Matchers {

  it should "be compatible as is when the reversed serializer is compatible" in {
    val registeredSnapshot = reversedStringOrderingSerializer.snapshotConfiguration()
    val restoredSnapshot   = savepointed[Ordering[String]](reversedStringOrderingSerializer)

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when the reversed serializer is incompatible" in {
    val registeredSnapshot = reversedStringOrderingSerializer.snapshotConfiguration()
    val restoredSnapshot   =
      savepointed[Ordering[String]](new ReverseOrderingSerializer[String](reversedStringOrderingSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  private def reversedStringOrderingSerializer = new ReverseOrderingSerializer[String](DefaultStringOrderingSerializer)

}
