package org.apache.flinkx.api

import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.DerivationCacheTest._
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.scalatest.concurrent.Eventually._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.lang.ref.WeakReference
import java.net.URLClassLoader
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

class DerivationCacheTest extends AnyFlatSpec with Matchers {

  it should "expose the same derivation cache at each access of an entry point" in {
    auto.cache should be theSameInstanceAs auto.cache
  }

  it should "expose the derivation cache the type information are derived in" in {
    auto.cache.clear()
    auto.cache shouldBe empty

    // Held until checked: an entry is kept only while its type information is in use
    val derived = implicitly[TypeInformation[Item]]

    auto.cache.keySet.map(_.typeClass) should contain(derived.getTypeClass)
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

  // In a Flink session cluster sharing this library between jobs, each job loads its own classes: a class of the same
  // name loaded by another class loader is another type, and must get an entry of its own
  it should "key the cache by class as well as by type name" in {
    auto.cache.clear()
    val derived  = implicitly[TypeInformation[Item]]
    val itemInfo = implicitly[TypeInformation[String]]

    val otherJob  = new IsolatingClassLoader(classOf[Item].getProtectionDomain.getCodeSource.getLocation)
    val otherItem = otherJob.loadClass(classOf[Item].getName)
    otherItem.getName shouldBe classOf[Item].getName

    auto.cache.keySet.map(_.typeClass) should contain(derived.getTypeClass)
    auto.cache.keySet.map(_.typeClass) should not contain otherItem
    DerivationCacheKey(otherItem, "Item", Seq(itemInfo)) should not be DerivationCacheKey(
      classOf[Item],
      "Item",
      Seq(itemInfo)
    )
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

  // In a Flink session cluster sharing this library between jobs, a finished job must leave no class behind
  it should "release the class loader of a job once its type information are no longer used" in {
    // Scala 2 runtime reflection keeps the first class loader it reflects on: make it this one
    implicitly[TypeInformation[(List[Item], Int)]]
    val jobClassLoader = deriveInAnotherJob()

    eventually(timeout(Span(30, Seconds)), interval(Span(100, Millis))) {
      System.gc()
      // Compared to null here: a failed assertion holding the loader would keep it alive
      (jobClassLoader.get == null) shouldBe true
    }
  }

  /** Derives the type information of a tuple of the classes of another job, then forgets everything but its loader. */
  private def deriveInAnotherJob(): WeakReference[ClassLoader] = {
    val otherJob = new IsolatingClassLoader(classOf[Item].getProtectionDomain.getCodeSource.getLocation)
    val derive   = otherJob.loadClass(classOf[DeriveInJob].getName).getDeclaredConstructor().newInstance()
    val info     = derive.asInstanceOf[() => TypeInformation[_]]()
    auto.cache.keySet.map(_.typeClass.getClassLoader) should contain(otherJob)
    info.getTypeClass shouldBe classOf[(_, _)]
    new WeakReference(otherJob)
  }

  /** Loads the test classes itself rather than delegating, as the class loader of a job does for its own classes. */
  private class IsolatingClassLoader(jar: java.net.URL) extends URLClassLoader(Array(jar), getClass.getClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] =
      if (name.startsWith(classOf[DerivationCacheTest].getName))
        Option(findLoadedClass(name)).getOrElse(findClass(name))
      else super.loadClass(name, resolve)
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

  // A tuple is a class of the Scala library, nesting a class of the job in a collection its key doesn't name
  class DeriveInJob extends (() => TypeInformation[_]) {
    override def apply(): TypeInformation[_] = implicitly[TypeInformation[(List[Item], Int)]]
  }

}
