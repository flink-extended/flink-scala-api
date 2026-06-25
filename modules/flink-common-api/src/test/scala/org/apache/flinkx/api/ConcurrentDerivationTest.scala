package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.ConcurrentDerivationTest._
import org.apache.flinkx.api.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

class ConcurrentDerivationTest extends AnyFlatSpec with Matchers {

  it should "produce a singleton TypeInformation per type even when several threads derive types sharing subtypes" in {
    val threads    = 16
    val iterations = 50

    val tasks: Seq[() => TypeInformation[_]] = Seq(
      () => implicitly[TypeInformation[Holder1]],
      () => implicitly[TypeInformation[Holder2]],
      () => implicitly[TypeInformation[Holder3]],
      () => implicitly[TypeInformation[Holder4]],
      () => implicitly[TypeInformation[Holder5]],
      () => implicitly[TypeInformation[Holder6]],
      () => implicitly[TypeInformation[Holder7]],
      () => implicitly[TypeInformation[Holder8]]
    )

    (1 to iterations).foreach { _ =>
      // Clear the cache at each iteration to force re-derivation
      auto.cache.clear()

      val pool  = Executors.newFixedThreadPool(threads)
      val start = new CountDownLatch(1)
      try {
        val futures = (0 until threads).map { i =>
          pool.submit(new Callable[(Int, TypeInformation[_])] {
            override def call(): (Int, TypeInformation[_]) = {
              start.await()
              val taskIndex = i % tasks.size
              (taskIndex, tasks(taskIndex)())
            }
          })
        }
        start.countDown()
        val results = futures.map(_.get(30, TimeUnit.SECONDS))
        results should have size threads.toLong

        // Cache identity must be preserved across threads
        results.groupBy(_._1).values.foreach { sameTypeResults =>
          val tis = sameTypeResults.map(_._2)
          all(tis) should be theSameInstanceAs tis.head
        }
      } finally {
        pool.shutdownNow()
      }
    }
  }

}

object ConcurrentDerivationTest {

  case class SharedA(a: Int, b: String)
  case class SharedB(c: Long, d: Double)

  // Distinct top-level types sharing the same subtypes, so concurrent derivations race on the shared subtypes.
  case class Holder1(s1: SharedA, s2: SharedB, t: (Int, String))
  case class Holder2(s1: SharedA, s2: SharedB, t: (Int, String))
  case class Holder3(s1: SharedA, s2: SharedB, t: (Int, String))
  case class Holder4(s1: SharedA, s2: SharedB, t: (Int, String))
  case class Holder5(s1: SharedA, s2: SharedB, t: (Int, String))
  case class Holder6(s1: SharedA, s2: SharedB, t: (Int, String))
  case class Holder7(s1: SharedA, s2: SharedB, t: (Int, String))
  case class Holder8(s1: SharedA, s2: SharedB, t: (Int, String))

}
