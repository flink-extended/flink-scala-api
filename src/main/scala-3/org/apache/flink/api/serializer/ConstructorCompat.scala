package org.apache.flink.api.serializer

import java.lang.reflect.Modifier

import scala.util.control.NonFatal

private[serializer] trait ConstructorCompat:
  // As of Scala version 3.1.2, there is no direct support for runtime reflection.
  // This is in contrast to Scala 2, which has its own APIs for reflecting on classes.
  // Thus, fallback to Java reflection and look up the constructor matching the required signature.
  final def lookupConstructor[T](cls: Class[T], numFields: Int): Array[AnyRef] => T =
    // Types of parameters can fail to match when (un)boxing is used.
    // Say you have a class `final case class Foo(a: String, b: Int)`.
    // The first parameter is an alias for `java.lang.String`, which the constructor uses.
    // The second parameter is an alias for `java.lang.Integer`, but the constructor actually takes an unboxed `int`.
    val constructor = try
      cls.getConstructors
        .collectFirst { case c if c.getParameterCount == numFields => c }
        .get
    catch case NonFatal(e) =>
      throw new IllegalArgumentException(
        s"""
           |The class ${cls.getSimpleName} does not have a matching constructor.
           |It could be an instance class, meaning it is not a member of a
           |toplevel object, or of an object contained in a toplevel object,
           |therefore it requires an outer instance to be instantiated, but we don't have a
           |reference to the outer instance. Please consider changing the outer class to an object.
           |""".stripMargin,
        e
      )

    { (arr: Array[AnyRef]) => constructor.newInstance(arr*).asInstanceOf[T] }
