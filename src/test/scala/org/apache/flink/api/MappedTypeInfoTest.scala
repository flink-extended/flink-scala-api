package org.apache.flink

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flink.MappedTypeInfoTest.WrappedString
import org.apache.flink.api.serializers._
import org.apache.flink.api.serializer.MappedSerializer.TypeMapper
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.reflect.ClassTag

class MappedTypeInfoTest extends AnyFlatSpec with Matchers with TestUtils {
  import MappedTypeInfoTest._
  it should "derive TI for non-serializeable classes" in {
    drop(implicitly[TypeInformation[WrappedString]])
  }
}

object MappedTypeInfoTest {
  class WrappedMapper extends TypeMapper[WrappedString, String] {
    override def map(a: WrappedString): String = a.get

    override def contramap(b: String): WrappedString = {
      val str = new WrappedString
      str.put(b)
      str
    }
  }
  implicit val mapper: TypeMapper[WrappedString, String] = new WrappedMapper()

  class WrappedString {
    private var internal: String = ""

    override def equals(obj: Any): Boolean = obj match {
      case s: WrappedString => s.get == internal
      case _                => false
    }
    def get: String = internal
    def put(value: String) = {
      internal = value
    }
  }
}
