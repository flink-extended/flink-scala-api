package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.semiauto.stringSerializer
import org.apache.flinkx.api.serializer.SnapshotTestUtil._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedMap

class SortedMapSerializerTest extends AnyFlatSpec with Matchers {

  it should "be compatible as is when the nested serializers are compatible" in {
    val registeredSnapshot = sortedMapSerializer(fooSerializer).snapshotConfiguration()
    val restoredSnapshot   = savepointed[SortedMap[String, Foo]](sortedMapSerializer(fooSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when the serializer of the values is incompatible" in {
    val registeredSnapshot = sortedMapSerializer(fooSerializer).snapshotConfiguration()
    val restoredSnapshot   = savepointed[SortedMap[String, Foo]](sortedMapSerializer(barSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  private def sortedMapSerializer[V](valueSerializer: TypeSerializer[V]) =
    new SortedMapSerializer[String, V](stringSerializer, valueSerializer, DefaultStringOrderingSerializer)

}
