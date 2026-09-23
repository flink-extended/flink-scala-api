package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.semiauto.stringSerializer
import org.apache.flinkx.api.serializer.SerializerUtil.resolveNestedSchemaCompatibility
import org.apache.flinkx.api.serializer.SerializerUtilTest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Checks the cases no serializer of this library can produce, so that no snapshot test covers them. */
class SerializerUtilTest extends AnyFlatSpec with Matchers {

  it should "be incompatible when the serializers do not nest the same number of serializers" in {
    val compatibility = resolveNestedSchemaCompatibility(
      Array(compatibleAsIsSnapshot, compatibleAsIsSnapshot),
      Array(compatibleAsIsSnapshot),
      restoreStringSerializer
    )

    compatibility shouldBe Symbol("incompatible")
  }

  it should "be compatible after migration when a nested serializer is compatible after migration" in {
    val compatibility = resolveNestedSchemaCompatibility(
      Array(compatibleAsIsSnapshot, compatibleAfterMigrationSnapshot),
      Array(compatibleAsIsSnapshot, compatibleAsIsSnapshot),
      restoreStringSerializer
    )

    compatibility shouldBe Symbol("compatibleAfterMigration")
  }

  private def restoreStringSerializer(nestedSerializers: Array[TypeSerializer[_]]): TypeSerializer[String] =
    stringSerializer

}

object SerializerUtilTest {

  private def compatibleAsIsSnapshot = new FixedCompatibilitySnapshot(
    TypeSerializerSchemaCompatibility.compatibleAsIs()
  )

  private def compatibleAfterMigrationSnapshot = new FixedCompatibilitySnapshot(
    TypeSerializerSchemaCompatibility.compatibleAfterMigration()
  )

  /** Snapshot of [[String]] reporting the compatibility it is built with, whatever it is resolved against. */
  private class FixedCompatibilitySnapshot(compatibility: TypeSerializerSchemaCompatibility[String])
      extends TypeSerializerSnapshot[String] {

    override def getCurrentVersion: Int = 1

    override def writeSnapshot(out: DataOutputView): Unit = {}

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {}

    override def restoreSerializer(): TypeSerializer[String] = stringSerializer

    override def resolveSchemaCompatibility(
        restoredSnapshot: TypeSerializerSnapshot[String]
    ): TypeSerializerSchemaCompatibility[String] = compatibility

  }

}
