package org.apache.flinkx.api.serializer

import org.apache.flink.api.java.typeutils.runtime.RowSerializer
import org.apache.flink.types.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flinkx.api.semiauto.intSerializer

class ArraySerializerTest extends AnyFlatSpec with Matchers {

  it should "copy to new array with same immutable elements" in {
    val intArraySerializer = new ArraySerializer[Int](intSerializer, classOf[Int])
    val expectedData       = Array(1, 2, 3)

    val resultData = intArraySerializer.copy(expectedData)
    resultData shouldNot be theSameInstanceAs expectedData
    resultData shouldEqual expectedData
  }

  it should "copy to new array with a copy of mutable elements" in {
    val intArraySerializer        = new ArraySerializer[Int](intSerializer, classOf[Int])
    val arrayOfIntArraySerializer = new ArraySerializer[Array[Int]](intArraySerializer, classOf[Array[Int]])
    val expectedData              = Array(Array(1, 2), Array(3, 4))

    val resultData = arrayOfIntArraySerializer.copy(expectedData)
    resultData shouldNot be theSameInstanceAs expectedData
    resultData(0) shouldNot be theSameInstanceAs expectedData(0)
    resultData(1) shouldNot be theSameInstanceAs expectedData(1)
    resultData shouldEqual expectedData
  }

  it should "return itself when duplicate an immutable serializer" in {
    val intArraySerializer = new ArraySerializer[Int](intSerializer, classOf[Int])

    val duplicatedIntArraySerializer = intArraySerializer.duplicate()
    duplicatedIntArraySerializer should be theSameInstanceAs intArraySerializer
  }

  it should "duplicate itself when the serializer is mutable" in {
    val rowArraySerializer = new ArraySerializer[Row](new RowSerializer(Array.empty), classOf[Row])

    val duplicatedIntArraySerializer = rowArraySerializer.duplicate()

    duplicatedIntArraySerializer shouldNot be theSameInstanceAs rowArraySerializer
    duplicatedIntArraySerializer should be(rowArraySerializer)
  }

}
