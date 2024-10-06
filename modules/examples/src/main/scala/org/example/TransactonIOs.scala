package org.example

import org.apache.flink.streaming.api.functions.source.FromIteratorFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context

import org.slf4j.LoggerFactory

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import java.sql.Timestamp

case class Transaction(
    accountId: Long,
    timestamp: Long,
    amount: Double,
    loc: String = ""
)

object TransactionsSource:
  private val data =
    Array(
      Transaction(1, 0L, 188.23),
      Transaction(2, 0L, 374.79),
      Transaction(3, 0L, 112.15),
      Transaction(4, 0L, 478.75),
      Transaction(5, 0L, 208.85),
      Transaction(1, 0L, 379.64),
      Transaction(2, 0L, 351.44),
      Transaction(3, 0L, 320.75),
      Transaction(4, 0L, 259.42),
      Transaction(5, 0L, 273.44),
      Transaction(1, 0L, 267.25),
      Transaction(2, 0L, 397.15),
      Transaction(3, 0L, 0.219),
      Transaction(4, 0L, 231.94),
      Transaction(5, 0L, 384.73),
      Transaction(1, 0L, 419.62),
      Transaction(2, 0L, 412.91),
      Transaction(3, 0L, 0.77),
      Transaction(4, 0L, 22.10),
      Transaction(5, 0L, 377.54),
      Transaction(1, 0L, 375.44),
      Transaction(2, 0L, 230.18),
      Transaction(3, 0L, 0.80),
      Transaction(4, 0L, 350.89),
      Transaction(5, 0L, 127.55),
      Transaction(1, 0L, 483.91),
      Transaction(2, 0L, 228.22),
      Transaction(3, 0L, 871.15),
      Transaction(4, 0L, 64.19),
      Transaction(5, 0L, 79.43),
      Transaction(1, 0L, 56.12),
      Transaction(2, 0L, 256.48),
      Transaction(3, 0L, 148.16),
      Transaction(4, 0L, 199.95),
      Transaction(5, 0L, 252.37),
      Transaction(1, 0L, 274.73),
      Transaction(2, 0L, 473.54),
      Transaction(3, 0L, 119.92),
      Transaction(4, 0L, 323.59),
      Transaction(5, 0L, 353.16),
      Transaction(1, 0L, 211.90),
      Transaction(2, 0L, 280.93),
      Transaction(3, 0L, 347.89),
      Transaction(4, 0L, 459.86),
      Transaction(5, 0L, 82.31),
      Transaction(1, 0L, 373.26),
      Transaction(2, 0L, 479.83),
      Transaction(3, 0L, 454.25),
      Transaction(4, 0L, 83.64),
      Transaction(5, 0L, 292.44)
    )

  def iterator: FromIteratorFunction[Transaction] =
    FromIteratorFunction[Transaction](
      (new Iterator[Transaction] with Serializable:
        var rows = data.iterator
        var timestamp = Timestamp.valueOf("2019-01-01 00:00:00").getTime
        val sixMinutes = 6.minutes.toMillis

        override def hasNext: Boolean = rows.hasNext

        override def next(): Transaction =
          Thread.sleep(100)
          val next = rows.next

          // Going to the first element again
          if !hasNext then rows = data.iterator
          // Moving timestamp further
          timestamp += sixMinutes

          next.copy(timestamp = timestamp)
      ).asJava
    )

case class Alert(id: Long)

class AlertSink extends SinkFunction[Alert]:
  private val logger = LoggerFactory.getLogger(classOf[AlertSink])

  override def invoke(value: Alert, context: Context): Unit =
    logger.info(value.toString)
