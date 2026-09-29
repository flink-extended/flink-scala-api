package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.DerivationCache.{Entry, Key}

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

/** Cache of the type information derived by this library, exposed by `TypeInformationDerivation.cache`.
  *
  * An entry is kept only while its type information is in use: the classes of a job of a Flink session cluster sharing
  * this library are released with the job.
  */
final class DerivationCache private[api] () {

  private val entries  = new ConcurrentHashMap[Key, Entry]()
  private val released = new ReferenceQueue[TypeInformation[_]]()

  /** The type information cached for the given key, if any. */
  def get(key: DerivationCacheKey): Option[TypeInformation[_]] = {
    removeReleased()
    Option(entries.get(new Key(key))).flatMap(entry => Option(entry.get))
  }

  /** Caches the given type information unless another one is cached for the given key, and returns that other one. */
  def putIfAbsent(key: DerivationCacheKey, info: TypeInformation[_]): Option[TypeInformation[_]] = {
    removeReleased()
    val entry = new Entry(info, new Key(key), released)

    @tailrec def put(): Option[TypeInformation[_]] = Option(entries.putIfAbsent(entry.key, entry)) match {
      case None         => None
      case Some(cached) =>
        Option(cached.get) match {
          // The cached type information is no longer in use, but its entry is not removed yet
          case None => if (entries.replace(entry.key, cached, entry)) None else put()
          case some => some
        }
    }

    put()
  }

  /** Keys of the type information cached. */
  def keySet: Set[DerivationCacheKey] =
    entries.values.asScala.filter(_.get != null).flatMap(_.key.resolve).toSet

  def size: Int = keySet.size

  def isEmpty: Boolean = keySet.isEmpty

  def clear(): Unit = entries.clear()

  private def removeReleased(): Unit = {
    var entry = released.poll()
    while (entry != null) {
      val releasedEntry = entry.asInstanceOf[Entry]
      entries.remove(releasedEntry.key, releasedEntry)
      entry = released.poll()
    }
  }

}

object DerivationCache {

  /** A [[DerivationCacheKey]] that doesn't keep its class and type information alive. */
  private final class Key(key: DerivationCacheKey) {
    private val typeClass = new WeakReference[Class[_]](key.typeClass)
    private val typeName  = key.typeName
    private val members   = key.memberTypeInfos.map(new WeakReference[TypeInformation[_]](_))

    override val hashCode: Int = key.hashCode

    /** The key it was made of, unless its class or one of its type information is no longer in use. */
    def resolve: Option[DerivationCacheKey] = {
      val clazz = typeClass.get
      val infos = members.map(_.get)
      if (clazz == null || infos.contains(null)) None else Some(DerivationCacheKey(clazz, typeName, infos))
    }

    override def equals(other: Any): Boolean = other match {
      case that: Key => (this eq that) || (hashCode == that.hashCode && resolve.exists(that.resolve.contains))
      case _         => false
    }
  }

  private final class Entry(info: TypeInformation[_], val key: Key, released: ReferenceQueue[TypeInformation[_]])
      extends WeakReference[TypeInformation[_]](info, released)

}
