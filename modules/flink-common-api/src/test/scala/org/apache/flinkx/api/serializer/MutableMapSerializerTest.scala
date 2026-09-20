package org.apache.flinkx.api.serializer

import org.apache.flinkx.api.semiauto.stringSerializer
import org.apache.flinkx.api.serializer.SnapshotTestUtil._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class MutableMapSerializerTest extends AnyFlatSpec with Matchers {

  it should "be compatible as is when the serializer of the values is compatible" in {
    val registeredSnapshot = new MutableMapSerializer(stringSerializer, fooSerializer).snapshotConfiguration()
    val restoredSnapshot   =
      savepointed[mutable.Map[String, Foo]](new MutableMapSerializer(stringSerializer, fooSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when the serializer of the values is incompatible" in {
    val registeredSnapshot = new MutableMapSerializer(stringSerializer, fooSerializer).snapshotConfiguration()
    val restoredSnapshot   =
      savepointed[mutable.Map[String, Foo]](new MutableMapSerializer(stringSerializer, barSerializer))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

}
