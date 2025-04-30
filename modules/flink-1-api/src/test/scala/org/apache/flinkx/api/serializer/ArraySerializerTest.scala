package org.apache.flinkx.api.serializer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ArraySerializerTest extends AnyFlatSpec with Matchers {

  it should "copy to new array with same immutable elements" in {
    val intArraySerializer = new ArraySerializer[Int](org.apache.flinkx.api.serializers.intSerializer, classOf[Int])
    val expectedData       = Array(1, 2, 3)

    val resultData = intArraySerializer.copy(expectedData)
    resultData shouldNot be theSameInstanceAs expectedData
    resultData shouldEqual expectedData
  }

  it should "copy to new array with a copy of mutable elements" in {
    val intArraySerializer = new ArraySerializer[Int](org.apache.flinkx.api.serializers.intSerializer, classOf[Int])
    val arrayOfIntArraySerializer = new ArraySerializer[Array[Int]](intArraySerializer, classOf[Array[Int]])
    val expectedData              = Array(Array(1, 2), Array(3, 4))

    val resultData = arrayOfIntArraySerializer.copy(expectedData)
    resultData shouldNot be theSameInstanceAs expectedData
    resultData(0) shouldNot be theSameInstanceAs expectedData(0)
    resultData(1) shouldNot be theSameInstanceAs expectedData(1)
    resultData shouldEqual expectedData
  }

}
