package org.apache.flinkx.api.util

import java.lang.reflect.{Field, Modifier}

object ClassUtil {

  /** Checks if given case class is immutable by checking if all its parameters are val (final fields).
    *
    * @param clazz
    *   The case class to check.
    * @param paramNames
    *   The names of the case class parameters.
    * @return
    *   true if all the case class parameters are immutable, false otherwise.
    */
  def isCaseClassImmutable(clazz: Class[_], paramNames: Seq[String]): Boolean = {
    val declaredFields: Array[Field] = clazz.getDeclaredFields
    paramNames.forall(paramName =>
      declaredFields
        .find(field => field.getName == paramName)
        .orElse(declaredFields.find(field => field.getName == s"${clazz.getName.replace('.', '$')}$$$$$paramName"))
        // return true if the field isn't found in the class: case classes can only override val from parent classes
        .forall(field => Modifier.isFinal(field.getModifiers))
    )
  }

}
