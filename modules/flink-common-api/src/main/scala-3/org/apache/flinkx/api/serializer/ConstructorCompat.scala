package org.apache.flinkx.api.serializer

import java.lang.reflect.Method
import scala.util.control.NonFatal

private[serializer] trait ConstructorCompat:
  // As of Scala version 3.1.2, there is no direct support for runtime reflection.
  // This is in contrast to Scala 2, which has its own APIs for reflecting on classes.
  // Thus, fallback to Java reflection and look up the apply method matching the required signature.
  // Apply method is used because enum case classes cannot be instantiated by its constructor
  final def lookupConstructor[T](cls: Class[T]): Array[AnyRef] => T =
    // Types of parameters can fail to match when (un)boxing is used.
    // Say you have a class `final case class Foo(a: String, b: Int)`.
    // The first parameter is an alias for `java.lang.String`, which the apply uses.
    // The second parameter is an alias for `java.lang.Integer`, but the apply actually takes an unboxed `int`.
    val applyMethod =
      try
        cls.getMethods
          .filter(_.getName == "apply")
          .foldLeft[Option[Method]](None) {
            case (Some(longest), c) if longest.getParameterCount < c.getParameterCount => Some(c)
            case (_, c)                                                                => Some(c)
          }
          .get
      catch
        case NonFatal(e) =>
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

    lazy val defaultArgs = cls.getMethods
      .filter(_.getName.startsWith("$lessinit$greater$default"))
      .sortBy(_.getName())
      .map(_.invoke(null))

    (args: Array[AnyRef]) => {
      // Append default values for missing arguments
      val allArgs = args ++ defaultArgs.takeRight(applyMethod.getParameterCount - args.length)
      applyMethod.invoke(null, allArgs*).asInstanceOf[T]
    }
