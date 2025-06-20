package org.apache.flinkx.api.util

import java.lang.reflect.{Field, Modifier}
import scala.annotation.tailrec

object ClassUtil {

  /** Checks if a field is final in the given class or its superclasses.
    *
    * @param declaredFields
    *   The array of fields declared in the class. It's an optimization for the caller to ask for the declared fields.
    * @param clazz
    *   The class to check for the field.
    * @param fieldName
    *   The name of the field to check.
    * @return
    *   true if the field is final, false otherwise.
    * @throws NoSuchFieldException
    *   if the field does not exist in the class or its superclasses.
    */
  def isFieldFinal(declaredFields: Array[Field], clazz: Class[_], fieldName: String): Boolean =
    Modifier.isFinal(findField(declaredFields, clazz, fieldName).getModifiers)

  @tailrec
  private def findField(declaredFields: Array[Field], clazz: Class[_], fieldName: String): Field = {
    val fieldOption = declaredFields
      .find(f => f.getName == fieldName)
      .orElse(declaredFields.find(f => f.getName == s"${clazz.getName.replace('.', '$')}$$$$$fieldName"))
    if (fieldOption.nonEmpty) {
      fieldOption.get
    } else {
      val superclass = clazz.getSuperclass
      if (superclass == null || classOf[Object] == superclass) {
        throw new NoSuchFieldException(fieldName) // Same as Class.getDeclaredField
      } else {
        findField(superclass.getDeclaredFields, superclass, fieldName)
      }
    }
  }

}
