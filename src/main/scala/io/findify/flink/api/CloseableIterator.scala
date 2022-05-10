package io.findify.flink.api

import org.apache.flink.util.{CloseableIterator => JCloseableIterator}

/** This interface represents an [[Iterator]] that is also [[AutoCloseable]]. A typical use-case for this interface are
  * iterators that are based on native-resources such as files, network, or database connections. Clients must call
  * close after using the iterator.
  */
trait CloseableIterator[T] extends Iterator[T] with AutoCloseable {}

object CloseableIterator {

  def fromJava[T](it: JCloseableIterator[T]): CloseableIterator[T] =
    new CloseableIterator[T] {
      override def hasNext: Boolean = it.hasNext

      override def next(): T = it.next

      override def close(): Unit = it.close()
    }
}
