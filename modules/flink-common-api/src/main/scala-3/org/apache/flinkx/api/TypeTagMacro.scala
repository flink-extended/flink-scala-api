package org.apache.flinkx.api

import scala.quoted.*

// Simple macro to derive a TypeTag for any type.
object TypeTagMacro:
  def gen[A: Type](using q: Quotes): Expr[TypeTag[A]] =
    import q.reflect.*

    def check(r: TypeRepr): Boolean =
      r match {
        case a: AppliedType =>
          !a.args.exists { t => t.typeSymbol.isAbstractType } && a.args.forall { t => check(t) }
        case _ => true
      }

    val A              = TypeRepr.of[A]
    val symA           = A.typeSymbol
    val flagsA         = symA.flags
    val isModuleExpr   = Expr(flagsA.is(Flags.Module))
    val isCachableExpr = Expr(check(A))

    val toStringExpr = Expr(A.show)

    '{
      new TypeTag[A]:
        override lazy val isModule: Boolean   = ${ isModuleExpr }
        override lazy val isCachable: Boolean = ${ isCachableExpr }
        override lazy val toString: String    = ${ toStringExpr }
    }
