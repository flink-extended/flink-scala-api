package org.apache.flinkx.api.serializer

import scala.annotation.nowarn
import scala.reflect.runtime.{currentMirror => cm}
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

import org.apache.flinkx.api.serializers.drop

private[serializer] trait ConstructorCompat {
  // # fields is required for the Scala 3 implementation, so need to match the method signature here.
  @nowarn("msg=(eliminated by erasure)|(explicit array)")
  final def lookupConstructor[T](cls: Class[T], numFields: Int): Array[AnyRef] => T = {
    drop(numFields)
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

    val classMirror             = rootMirror.reflectClass(classSymbol)
    val constructorMethodMirror = classMirror.reflectConstructor(primaryConstructorSymbol)

    val claas  = cm.classSymbol(cls)
    val module = claas.companion.asModule
    val im     = cm.reflect(cm.reflectModule(module).instance)

    def withDefault(im: InstanceMirror, name: String, givenArgs: Int): List[Any] = {
      val at     = TermName(name)
      val ts     = im.symbol.typeSignature
      val method = ts.member(at).asMethod

      // either defarg or default val for type of p
      def valueFor(p: Symbol, i: Int): Any = {
        val defarg = ts member TermName(s"$name$$default$$${i + 1}")
        if (defarg != NoSymbol)
          im.reflectMethod(defarg.asMethod)()
        else
          p.typeSignature match {
            case t if t =:= typeOf[String]                                                                => null
            case t if t =:= typeOf[Int] | t =:= typeOf[Long] | t =:= typeOf[Double] | t =:= typeOf[Float] => 0
            case x => throw new IllegalArgumentException(x.toString)
          }
      }

      val defaultArgs = method.paramLists.flatten.splitAt(givenArgs)._2
      defaultArgs.zipWithIndex.map(p => valueFor(p._1, p._2 + givenArgs))
    }

    { (args: Array[AnyRef]) =>
      {
        val allArgs = args.toList ++ withDefault(im, "apply", args.length)
        constructorMethodMirror.apply(allArgs: _*).asInstanceOf[T]
      }
    }
  }
}
