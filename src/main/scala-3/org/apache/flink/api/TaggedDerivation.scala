package org.apache.flinkx.api

import scala.compiletime.*
import scala.deriving.Mirror
import scala.reflect.*

import magnolia1.{CallByNeed, CaseClass, SealedTrait, Monadic}
import magnolia1.Macro.*

// Typeclass derivation providing `ClassTag` and `TypeTag` givens.
// Copied & modified from Magnolia, since the Scala 3 version disallows adding constraints to `join` and `split`.
trait CommonTaggedDerivation[TypeClass[_]]:
  type Typeclass[T] = TypeClass[T]

  def join[T](ctx: CaseClass[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): Typeclass[T]

  inline def derivedMirrorProduct[A](product: Mirror.ProductOf[A])(using
      ClassTag[A],
      TypeTag[A]
  ): Typeclass[A] =
    val parameters = IArray(
      getParams_[A, product.MirroredElemLabels, product.MirroredElemTypes](
        paramAnns[A].to(Map),
        inheritedParamAnns[A].to(Map),
        paramTypeAnns[A].to(Map),
        repeated[A].to(Map)
      )*
    )

    val caseClass = new CaseClass[Typeclass, A](
      typeInfo[A],
      isObject[A],
      isValueClass[A],
      parameters,
      IArray(anns[A]*),
      IArray(inheritedAnns[A]*),
      IArray[Any](typeAnns[A]*)
    ):

      def construct[PType](
          makeParam: Param => PType
      )(using ClassTag[PType]): A =
        product.fromProduct(
          Tuple.fromArray(this.params.map(makeParam(_)).to(Array))
        )

      def rawConstruct(fieldValues: Seq[Any]): A =
        product.fromProduct(Tuple.fromArray(fieldValues.to(Array)))

      def constructEither[Err, PType: ClassTag](
          makeParam: Param => Either[Err, PType]
      ): Either[List[Err], A] =
        params
          .map(makeParam(_))
          .to(Array)
          .foldLeft[Either[List[Err], Array[PType]]](Right(Array())) {
            case (Left(errs), Left(err))    => Left(errs ++ List(err))
            case (Right(acc), Right(param)) => Right(acc ++ Array(param))
            case (errs @ Left(_), _)        => errs
            case (_, Left(err))             => Left(List(err))
          }
          .map { params => product.fromProduct(Tuple.fromArray(params)) }

      def constructMonadic[M[_]: Monadic, PType: ClassTag](
          makeParam: Param => M[PType]
      ): M[A] =
        summon[Monadic[M]].map {
          params
            .map(makeParam(_))
            .to(Array)
            .foldLeft(summon[Monadic[M]].point(Array())) { (accM, paramM) =>
              summon[Monadic[M]].flatMap(accM) { acc =>
                summon[Monadic[M]].map(paramM)(acc ++ List(_))
              }
            }
        } { params => product.fromProduct(Tuple.fromArray(params)) }

    join(caseClass)

  inline def getParams_[T, Labels <: Tuple, Params <: Tuple](
      annotations: Map[String, List[Any]],
      inheritedAnnotations: Map[String, List[Any]],
      typeAnnotations: Map[String, List[Any]],
      repeated: Map[String, Boolean],
      idx: Int = 0
  ): List[CaseClass.Param[Typeclass, T]] =
    inline erasedValue[(Labels, Params)] match
      case _: (EmptyTuple, EmptyTuple) =>
        Nil
      case _: ((l *: ltail), (p *: ptail)) =>
        val label     = constValue[l].asInstanceOf[String]
        val typeclass = CallByNeed(summonInline[Typeclass[p]])

        CaseClass.Param[Typeclass, T, p](
          label,
          idx,
          repeated.getOrElse(label, false),
          typeclass,
          CallByNeed(None),
          IArray.from(annotations.getOrElse(label, List())),
          IArray.from(inheritedAnnotations.getOrElse(label, List())),
          IArray.from(typeAnnotations.getOrElse(label, List()))
        ) ::
          getParams_[T, ltail, ptail](
            annotations,
            inheritedAnnotations,
            typeAnnotations,
            repeated,
            idx + 1
          )

  // for backward compatibility with v1.0.0
  inline def getParams[T, Labels <: Tuple, Params <: Tuple](
      annotations: Map[String, List[Any]],
      typeAnnotations: Map[String, List[Any]],
      repeated: Map[String, Boolean],
      idx: Int = 0
  ): List[CaseClass.Param[Typeclass, T]] =
    getParams_(annotations, Map.empty, typeAnnotations, repeated, idx)

trait TaggedDerivation[TypeClass[_]] extends CommonTaggedDerivation[TypeClass]:
  def split[T](ctx: SealedTrait[Typeclass, T])(using
      classTag: ClassTag[T],
      typeTag: TypeTag[T]
  ): Typeclass[T]

  transparent inline def subtypes[T, SubtypeTuple <: Tuple](
      m: Mirror.SumOf[T],
      idx: Int = 0
  ): List[SealedTrait.Subtype[Typeclass, T, _]] =
    inline erasedValue[SubtypeTuple] match
      case _: EmptyTuple =>
        Nil
      case _: (s *: tail) =>
        new SealedTrait.Subtype(
          typeInfo[s],
          IArray.from(anns[s]),
          IArray.from(inheritedAnns[s]),
          IArray.from(paramTypeAnns[T]),
          isObject[s],
          idx,
          CallByNeed(summonFrom {
            case tc: Typeclass[`s`] =>
              tc

            case _ =>
              derived(using
                summonInline[Mirror.Of[s]],
                summonInline[ClassTag[s]],
                summonInline[TypeTag[s]]
              )
          }),
          x => m.ordinal(x) == idx,
          _.asInstanceOf[s & T]
        ) :: subtypes[T, tail](m, idx + 1)

  inline def derivedMirrorSum[A](sum: Mirror.SumOf[A])(using ClassTag[A], TypeTag[A]): Typeclass[A] =
    val sealedTrait = SealedTrait(
      typeInfo[A],
      IArray(subtypes[A, sum.MirroredElemTypes](sum)*),
      IArray.from(anns[A]),
      IArray(paramTypeAnns[A]*),
      isEnum[A],
      IArray.from(inheritedAnns[A])
    )

    split(sealedTrait)

  inline def derivedMirror[A](using mirror: Mirror.Of[A], c: ClassTag[A], t: TypeTag[A]): Typeclass[A] =
    inline mirror match
      case sum: Mirror.SumOf[A]         => derivedMirrorSum[A](sum)
      case product: Mirror.ProductOf[A] => derivedMirrorProduct[A](product)

  inline def derived[A](using Mirror.Of[A], ClassTag[A], TypeTag[A]): Typeclass[A] = derivedMirror[A]
