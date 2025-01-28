package org.apache.flinkx.api.typeinfo

import org.apache.flink.api.common.typeinfo.{TypeInfoFactory, TypeInformation}
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.typeinfo.FailFastTypeInfoFactory.formatType

import java.lang.reflect.Type
import java.util
import scala.jdk.CollectionConverters._

class FailFastTypeInfoFactory extends TypeInfoFactory[Nothing] {

  override def createTypeInfo(t: Type, params: util.Map[String, TypeInformation[_]]): TypeInformation[Nothing] =
    throw new FlinkRuntimeException(
      s"""You are using a 'Class' to resolve '${formatType(t, params)}' Scala type. flink-scala-api has no control over this kind of type resolution which may lead to silently fallback to generic Kryo serializers.
         |Use type information instead: import 'org.apache.flinkx.api.serializers._' to make implicitly available in the scope required 'TypeInformation' to resolve Scala types.
         |To disable this check, set 'DISABLE_FAIL_FAST_ON_SCALA_TYPE_RESOLUTION_WITH_CLASS' environment variable to 'true'.""".stripMargin
    )

}

object FailFastTypeInfoFactory {

  private def formatType(t: Type, params: util.Map[String, TypeInformation[_]]): String = if (params.isEmpty) {
    t.getTypeName
  } else {
    params.keySet().asScala.mkString(s"${t.getTypeName}[", ", ", "]")
  }

}
