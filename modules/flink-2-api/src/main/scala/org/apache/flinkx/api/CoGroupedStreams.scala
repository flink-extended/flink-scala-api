package org.apache.flinkx.api

import org.apache.flink.annotation.{PublicEvolving, Public}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{CoGroupedStreams => JavaCoGroupedStreams}
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector
import org.apache.flink.util.TaggedUnion
import ScalaStreamOps._
import scala.jdk.CollectionConverters._

/** `CoGroupedStreams` represents two [[DataStream]]s that have been co-grouped. A streaming co-group operation is
  * evaluated over elements in a window.
  *
  * To finalize the co-group operation you also need to specify a [[KeySelector]] for both the first and second input
  * and a [[WindowAssigner]]
  *
  * Note: Right now, the groups are being built in memory so you need to ensure that they don't get too big. Otherwise
  * the JVM might crash.
  *
  * Example:
  *
  * {{{
  * val one: DataStream[(String, Int)]  = ...
  * val two: DataStream[(String, Int)] = ...
  *
  * val result = one.coGroup(two)
  *     .where(new MyFirstKeySelector())
  *     .equalTo(new MyFirstKeySelector())
  *     .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
  *     .apply(new MyCoGroupFunction())
  * }
  * }}}
  */
@Public
class CoGroupedStreams[T1, T2](input1: DataStream[T1], input2: DataStream[T2]) {

  /** Specifies a [[KeySelector]] for elements from the first input.
    */
  def where[KEY: TypeInformation](keySelector: T1 => KEY): Where[KEY] = {
    val cleanFun     = clean(keySelector)
    val keyType      = implicitly[TypeInformation[KEY]]
    val javaSelector = new KeySelector[T1, KEY] with ResultTypeQueryable[KEY] {
      def getKey(in: T1)                                 = cleanFun(in)
      override def getProducedType: TypeInformation[KEY] = keyType
    }
    new Where[KEY](javaSelector, keyType)
  }

  /** A co-group operation that has [[KeySelector]]s defined for the first input.
    *
    * You need to specify a [[KeySelector]] for the second input using [[equalTo()]] before you can proceed with
    * specifying a [[WindowAssigner]] using [[EqualTo.window()]].
    *
    * @tparam KEY
    *   Type of the key. This must be the same for both inputs
    */
  class Where[KEY](keySelector1: KeySelector[T1, KEY], keyType: TypeInformation[KEY]) {

    /** Specifies a [[KeySelector]] for elements from the second input.
      */
    def equalTo(keySelector: T2 => KEY): EqualTo = {
      val cleanFun     = clean(keySelector)
      val localKeyType = keyType
      val javaSelector = new KeySelector[T2, KEY] with ResultTypeQueryable[KEY] {
        def getKey(in: T2)                                 = cleanFun(in)
        override def getProducedType: TypeInformation[KEY] = localKeyType
      }
      new EqualTo(javaSelector)
    }

    /** A co-group operation that a [[KeySelector]] defined for the first and the second input.
      *
      * A window can now be specified using [[window()]].
      */
    class EqualTo(keySelector2: KeySelector[T2, KEY]) {

      /** Specifies the window on which the co-group operation works.
        */
      @PublicEvolving
      def window[W <: Window](
          assigner: WindowAssigner[_ >: TaggedUnion[T1, T2], W]
      ): WithWindow[W] = {
        if (keySelector1 == null || keySelector2 == null) {
          throw new UnsupportedOperationException(
            "You first need to specify KeySelectors for both inputs using where() and equalTo()."
          )
        }
        new WithWindow[W](clean(assigner), null, null)
      }

      /** A co-group operation that has [[KeySelector]]s defined for both inputs as well as a [[WindowAssigner]].
        *
        * @tparam W
        *   Type of {@@linkWindow} on which the co-group operation works.
        */
      @PublicEvolving
      class WithWindow[W <: Window](
          windowAssigner: WindowAssigner[_ >: TaggedUnion[T1, T2], W],
          trigger: Trigger[_ >: TaggedUnion[T1, T2], _ >: W],
          evictor: Evictor[_ >: TaggedUnion[T1, T2], _ >: W]
      ) {

        /** Sets the [[Trigger]] that should be used to trigger window emission.
          */
        @PublicEvolving
        def trigger(newTrigger: Trigger[_ >: TaggedUnion[T1, T2], _ >: W]): WithWindow[W] = {
          new WithWindow[W](windowAssigner, newTrigger, evictor)
        }

        /** Sets the [[Evictor]] that should be used to evict elements from a window before emission.
          *
          * Note: When using an evictor window performance will degrade significantly, since pre-aggregation of window
          * results cannot be used.
          */
        @PublicEvolving
        def evictor(newEvictor: Evictor[_ >: TaggedUnion[T1, T2], _ >: W]): WithWindow[W] = {
          new WithWindow[W](windowAssigner, trigger, newEvictor)
        }

        /** Completes the co-group operation with the user function that is executed for windowed groups.
          */
        def apply[O: TypeInformation](fun: (Iterator[T1], Iterator[T2]) => O): DataStream[O] = {
          require(fun != null, "CoGroup function must not be null.")

          val coGrouper = new CoGroupFunction[T1, T2, O] {
            val cleanFun = clean(fun)
            def coGroup(
                left: java.lang.Iterable[T1],
                right: java.lang.Iterable[T2],
                out: Collector[O]
            ) = {
              out.collect(cleanFun(left.iterator().asScala, right.iterator().asScala))
            }
          }
          apply(coGrouper)
        }

        /** Completes the co-group operation with the user function that is executed for windowed groups.
          */
        def apply[O: TypeInformation](fun: (Iterator[T1], Iterator[T2], Collector[O]) => Unit): DataStream[O] = {
          require(fun != null, "CoGroup function must not be null.")

          val coGrouper = new CoGroupFunction[T1, T2, O] {
            val cleanFun = clean(fun)
            def coGroup(
                left: java.lang.Iterable[T1],
                right: java.lang.Iterable[T2],
                out: Collector[O]
            ) = {
              cleanFun(left.iterator.asScala, right.iterator.asScala, out)
            }
          }
          apply(coGrouper)
        }

        /** Completes the co-group operation with the user function that is executed for windowed groups.
          */
        def apply[T: TypeInformation](function: CoGroupFunction[T1, T2, T]): DataStream[T] = {

          val coGroup = new JavaCoGroupedStreams[T1, T2](input1.javaStream, input2.javaStream)

          asScalaStream(
            coGroup
              .where(keySelector1)
              .equalTo(keySelector2)
              .window(windowAssigner)
              .trigger(trigger)
              .evictor(evictor)
              .apply(clean(function), implicitly[TypeInformation[T]])
          )
        }
      }

    }
  }

  /** Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning is not disabled in the
    * [[org.apache.flink.api.common.ExecutionConfig]].
    */
  private[flinkx] def clean[F <: AnyRef](f: F): F = {
    new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
  }
}
