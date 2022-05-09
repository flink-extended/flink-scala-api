package io.findify.flink.api

import io.findify.flink.ClosureCleaner
import org.apache.flink.api.common.functions.{FlatJoinFunction, JoinFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{CoGroupedStreams, DataStream, JoinedStreams}
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

trait JoinedStreamsOps[T1, T2] {
  def stream: JoinedStreams[T1, T2]
  lazy val input1: DataStream[T1] = getUnderlyingStreamUnsafe("input1")
  lazy val input2: DataStream[T2] = getUnderlyingStreamUnsafe("input2")

  /** Pulls the underlying substreams via reflection. A terrible hack, but flink's JoinedStreams marks these fields
    * private
    * @param name
    * @tparam T
    * @return
    */
  private def getUnderlyingStreamUnsafe[T](name: String) = {
    val field = stream.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(stream).asInstanceOf[DataStream[T]]
  }

  /** Specifies a [[KeySelector]] for elements from the first input.
    */
  def where[KEY: TypeInformation](keySelector: T1 => KEY): Where[KEY] = {
    val cleanFun = ClosureCleaner.clean(keySelector)
    val keyType  = implicitly[TypeInformation[KEY]]
    val javaSelector = new KeySelector[T1, KEY] with ResultTypeQueryable[KEY] {
      def getKey(in: T1)                                 = cleanFun(in)
      override def getProducedType: TypeInformation[KEY] = keyType
    }
    new Where[KEY](javaSelector, keyType)
  }

  /** A join operation that has a [[KeySelector]] defined for the first input.
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
      val cleanFun     = ClosureCleaner.clean(keySelector)
      val localKeyType = keyType
      val javaSelector = new KeySelector[T2, KEY] with ResultTypeQueryable[KEY] {
        def getKey(in: T2)                                 = cleanFun(in)
        override def getProducedType: TypeInformation[KEY] = localKeyType
      }
      new EqualTo(javaSelector)
    }

    /** A join operation that has a [[KeySelector]] defined for the first and the second input.
      *
      * A window can now be specified using [[window()]].
      */
    class EqualTo(keySelector2: KeySelector[T2, KEY]) {

      /** Specifies the window on which the join operation works.
        */
      def window[W <: Window](
          assigner: WindowAssigner[_ >: CoGroupedStreams.TaggedUnion[T1, T2], W]
      ): WithWindow[W] = {
        if (keySelector1 == null || keySelector2 == null) {
          throw new UnsupportedOperationException(
            "You first need to specify KeySelectors for both inputs using where() and equalTo()."
          )
        }

        new WithWindow[W](ClosureCleaner.clean(assigner), null, null, null)
      }

      /** A join operation that has [[KeySelector]]s defined for both inputs as well as a [[WindowAssigner]].
        *
        * @tparam W
        *   Type of { @link Window} on which the join operation works.
        */
      class WithWindow[W <: Window](
          windowAssigner: WindowAssigner[_ >: CoGroupedStreams.TaggedUnion[T1, T2], W],
          trigger: Trigger[_ >: CoGroupedStreams.TaggedUnion[T1, T2], _ >: W],
          evictor: Evictor[_ >: CoGroupedStreams.TaggedUnion[T1, T2], _ >: W],
          val allowedLateness: Time
      ) {

        /** Sets the [[Trigger]] that should be used to trigger window emission.
          */
        def trigger(newTrigger: Trigger[_ >: CoGroupedStreams.TaggedUnion[T1, T2], _ >: W]): WithWindow[W] = {
          new WithWindow[W](windowAssigner, newTrigger, evictor, allowedLateness)
        }

        /** Sets the [[Evictor]] that should be used to evict elements from a window before emission.
          *
          * Note: When using an evictor window performance will degrade significantly, since pre-aggregation of window
          * results cannot be used.
          */
        def evictor(newEvictor: Evictor[_ >: CoGroupedStreams.TaggedUnion[T1, T2], _ >: W]): WithWindow[W] = {
          new WithWindow[W](windowAssigner, trigger, newEvictor, allowedLateness)
        }

        /** Sets the time by which elements are allowed to be late. Delegates to
          * [[WindowedStream#allowedLateness(Time)]]
          */
        def allowedLateness(newLateness: Time): WithWindow[W] = {
          new WithWindow[W](windowAssigner, trigger, evictor, newLateness)
        }

        /** Completes the join operation with the user function that is executed for windowed groups.
          */
        def apply[O: TypeInformation](fun: (T1, T2) => O): DataStream[O] = {
          require(fun != null, "Join function must not be null.")

          val joiner = new FlatJoinFunction[T1, T2, O] {
            val cleanFun = ClosureCleaner.clean(fun)
            def join(left: T1, right: T2, out: Collector[O]) = {
              out.collect(cleanFun(left, right))
            }
          }
          apply(joiner)
        }

        /** Completes the join operation with the user function that is executed for windowed groups.
          */
        def apply[O: TypeInformation](fun: (T1, T2, Collector[O]) => Unit): DataStream[O] = {
          require(fun != null, "Join function must not be null.")

          val joiner = new FlatJoinFunction[T1, T2, O] {
            val cleanFun = ClosureCleaner.clean(fun)
            def join(left: T1, right: T2, out: Collector[O]) = {
              cleanFun(left, right, out)
            }
          }
          apply(joiner)
        }

        /** Completes the join operation with the user function that is executed for windowed groups.
          */
        def apply[T: TypeInformation](function: JoinFunction[T1, T2, T]): DataStream[T] = {

          val join = new JoinedStreams[T1, T2](input1, input2)

          join
            .where(keySelector1)
            .equalTo(keySelector2)
            .window(windowAssigner)
            .trigger(trigger)
            .evictor(evictor)
            .allowedLateness(allowedLateness)
            .apply(ClosureCleaner.clean(function), implicitly[TypeInformation[T]])
        }

        /** Completes the join operation with the user function that is executed for windowed groups.
          */
        def apply[T: TypeInformation](function: FlatJoinFunction[T1, T2, T]): DataStream[T] = {

          val join = new JoinedStreams[T1, T2](input1, input2)

          join
            .where(keySelector1)
            .equalTo(keySelector2)
            .window(windowAssigner)
            .trigger(trigger)
            .evictor(evictor)
            .allowedLateness(allowedLateness)
            .apply(ClosureCleaner.clean(function), implicitly[TypeInformation[T]])

        }
      }
    }
  }

}
