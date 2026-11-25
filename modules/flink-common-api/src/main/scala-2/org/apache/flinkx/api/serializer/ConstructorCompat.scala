package org.apache.flinkx.api.serializer

import scala.annotation.nowarn
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe, currentMirror => cm}

private[serializer] trait ConstructorCompat {

  @nowarn("msg=(eliminated by erasure)|(explicit array)")
  final def lookupConstructor[T <: Product](cls: Class[T]): Array[AnyRef] => T = {
    val rootMirror  = universe.runtimeMirror(cls.getClassLoader)
    val classSymbol = rootMirror.classSymbol(cls)

    require(
      classSymbol.isStatic,
      s"""
         |The class ${cls.getSimpleName} is an instance class, meaning it is not a member of a
         |top level object, or of an object contained in a top level object,
         |therefore it requires an outer instance to be instantiated, but we don't have a
         |reference to the outer instance. Please consider changing the outer class to an object.
         |""".stripMargin
    )

    val primaryConstructorSymbol = classSymbol.toType
      .decl(universe.termNames.CONSTRUCTOR)
      .alternatives
      .collectFirst {
        case constructorSymbol: universe.MethodSymbol if constructorSymbol.isPrimaryConstructor =>
          constructorSymbol
      }
      .head
      .asMethod

    val classMirror     = rootMirror.reflectClass(classSymbol)
    val constructor     = classMirror.reflectConstructor(primaryConstructorSymbol)
    val claas           = cm.classSymbol(cls)
    val module          = claas.companion.asModule
    val im              = cm.reflect(cm.reflectModule(module).instance)
    val ts              = im.symbol.typeSignature
    val params          = primaryConstructorSymbol.paramLists.flatten
    val constructorSize = params.size
    val defaultValues   = (1 to constructorSize)
      .flatMap { i =>
        val defarg = ts.member(TermName(s"$$lessinit$$greater$$default$$$i"))
        if (defarg != NoSymbol)
          Some(im.reflectMethod(defarg.asMethod)())
        else None
      }

    // Java reflection is much faster, but it can neither unbox value classes nor tell same-arity constructors apart
    val javaConstructor = cls.getConstructors.filter(_.getParameterCount == constructorSize) match {
      case Array(c) if !params.exists(p => isValueClass(p.typeSignature)) => Some(c)
      case _                                                              => None
    }

    (args: Array[AnyRef]) => {
      // Append default values for missing arguments
      val allArgs =
        if (args.length == constructorSize) args
        else args ++ defaultValues.takeRight(constructorSize - args.length)
      javaConstructor match {
        case Some(c) => c.newInstance(allArgs: _*).asInstanceOf[T]
        case None    => constructor.apply(allArgs: _*).asInstanceOf[T]
      }
    }
  }

  private def isValueClass(tpe: Type): Boolean =
    tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isDerivedValueClass

}
