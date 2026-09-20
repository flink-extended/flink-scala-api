package org.apache.flinkx.api.serializer

import org.apache.flinkx.api.serializer.ScalaCaseObjectSerializerTest._
import org.apache.flinkx.api.serializer.SnapshotTestUtil._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ScalaCaseObjectSerializerTest extends AnyFlatSpec with Matchers {

  it should "be compatible as is when the same case object is serialized" in {
    val registeredSnapshot = caseObjectSerializer(Alpha).snapshotConfiguration()
    val restoredSnapshot   = savepointed[Alpha.type](caseObjectSerializer(Alpha))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when another case object is serialized" in {
    val registeredSnapshot = caseObjectSerializer(Alpha).snapshotConfiguration()
    val restoredSnapshot   = savepointed[Alpha.type](caseObjectSerializer(Beta))

    registeredSnapshot.resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol("incompatible")
  }

  private def caseObjectSerializer[T](caseObject: T) =
    new ScalaCaseObjectSerializer[T](caseObject.getClass.asInstanceOf[Class[T]])

}

object ScalaCaseObjectSerializerTest {
  case object Alpha
  case object Beta
}
