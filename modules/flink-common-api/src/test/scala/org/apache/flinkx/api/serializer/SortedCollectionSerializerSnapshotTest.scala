package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.semiauto.stringSerializer
import org.apache.flinkx.api.serializer.SnapshotTestUtil._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.SortedSet

class SortedCollectionSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "be compatible as is when the nested serializers are compatible" in {
    val registeredSnapshot = sortedSetSerializer(DefaultStringOrderingSerializer).snapshotConfiguration()
    val restoredSnapshot   = savepointed[SortedSet[String]](sortedSetSerializer(DefaultStringOrderingSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when the serializer of the ordering is incompatible" in {
    val registeredSnapshot = sortedSetSerializer(DefaultStringOrderingSerializer).snapshotConfiguration()
    val restoredSnapshot   = savepointed[SortedSet[String]](sortedSetSerializer(reversedStringOrderingSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  private def sortedSetSerializer(orderingSerializer: TypeSerializer[Ordering[String]]) =
    new SortedSetSerializer[String](stringSerializer, classOf[String], orderingSerializer)

  private def reversedStringOrderingSerializer = new ReverseOrderingSerializer[String](DefaultStringOrderingSerializer)

}
