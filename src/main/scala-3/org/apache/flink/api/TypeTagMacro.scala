package org.apache.flink.api

import scala.quoted.*

// Simple macro to derive a TypeTag for any type.
object TypeTagMacro:
  def gen[A: Type](using q: Quotes): Expr[TypeTag[A]] =
    import q.reflect.*

    val A = TypeRepr.of[A]
    val symA = A.typeSymbol
    val flagsA = symA.flags
    val isModuleExpr = Expr(flagsA.is(Flags.Module))

    '{
      new TypeTag[A]:
        override lazy val isModule: Boolean = ${isModuleExpr}
    }

