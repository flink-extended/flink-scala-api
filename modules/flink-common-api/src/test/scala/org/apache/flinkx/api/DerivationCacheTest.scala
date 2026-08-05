package org.apache.flinkx.api

import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.DerivationCacheTest._
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

class DerivationCacheTest extends AnyFlatSpec with Matchers {

  it should "expose the same derivation cache at each access of an entry point" in {
    auto.cache should be theSameInstanceAs auto.cache
  }

  it should "expose the derivation cache the type information are derived in" in {
    auto.cache.clear()
    auto.cache shouldBe empty

    implicitly[TypeInformation[Item]]

    auto.cache should not be empty
  }

  it should "not serve the type information cached to another one" in {
    val holderInfo = implicitly[TypeInformation[Holder]]

    val customHolderInfo = {
      implicit val itemInfo: TypeInformation[Item] = Types.GENERIC(classOf[Item])
      implicitly[TypeInformation[Holder]]
    }

    itemSerializerOf(holderInfo) shouldBe a[CaseClassSerializer[_]]
    // Fails when both Holders share the same cache keys: the type information derived first is served to the other
    itemSerializerOf(customHolderInfo) shouldNot be(a[CaseClassSerializer[_]])
  }

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

  private def itemSerializerOf(holderInfo: TypeInformation[Holder]): TypeSerializer[_] =
    holderInfo
      .createSerializer(new SerializerConfigImpl())
      .asInstanceOf[CaseClassSerializer[Holder]]
      .getFieldSerializers()(0)

}

object DerivationCacheTest {

  case class Item(id: String)
  case class Holder(item: Item)

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
