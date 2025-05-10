package org.apache.flinkx.api.util

import java.lang.reflect.{Field, Modifier}

object ClassUtil {

  def isFieldFinal(fields: Array[Field], className: String, fieldName: String): Boolean =
    Modifier.isFinal(
      fields
        .find(f => f.getName == fieldName)
        .orElse(fields.find(f => f.getName == s"${className.replace('.', '$')}$$$$$fieldName"))
        .getOrElse(throw new NoSuchFieldException(fieldName)) // Same as Class.getDeclaredField
        .getModifiers
    )

}
