package org.example

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.configuration.Configuration

import scala.util.Random
import scala.util.Try
import scala.util.Failure
import scala.util.Success

import java.util.Arrays

case class FakeKafkaRecord(
    timestamp: Long,
    key: Array[Byte],
    value: Array[Byte],
    partition: Int
)

object FakeKafkaSource:
  val NO_OF_PARTITIONS = 8

class FakeKafkaSource(
    seed: Int,
    idlePartitions: Set[Int],
    serializedMeasurements: Array[Array[Byte]],
    poisonPillRate: Double
) extends RichParallelSourceFunction[FakeKafkaRecord]:

  lazy val indexOfThisSubtask = getRuntimeContext.getIndexOfThisSubtask

  lazy val numberOfParallelSubtasks =
    getRuntimeContext.getNumberOfParallelSubtasks

  lazy val assignedPartitions =
    (0 to FakeKafkaSource.NO_OF_PARTITIONS).filter(
      _ % numberOfParallelSubtasks == indexOfThisSubtask
    )
  
  val rand = Random(seed)

  @transient @volatile var cancelled = false

  override def open(parameters: Configuration): Unit =
    println(s"Now reading from partitions: $assignedPartitions")
    
  override def run(ctx: SourceContext[FakeKafkaRecord]): Unit =
    if assignedPartitions.nonEmpty then
      while !cancelled do {
        val nextPartition = assignedPartitions(
          rand.nextInt(assignedPartitions.length)
        )

        if idlePartitions.contains(nextPartition) then
          // noinspection BusyWait
          Thread.sleep(1000) // avoid spinning wait
        else
          val nextTimestamp = getTimestampForPartition(nextPartition)

          var serializedMeasurement =
            serializedMeasurements(rand.nextInt(serializedMeasurements.length))

          if rand.nextFloat() > 1 - poisonPillRate then
            serializedMeasurement = Arrays.copyOf(serializedMeasurement, 10)

          (ctx.getCheckpointLock()).synchronized {
            ctx.collect(
              FakeKafkaRecord(
                nextTimestamp,
                Array.empty,
                serializedMeasurement,
                nextPartition
              )
            )
          }
      }
    else
      ctx.markAsTemporarilyIdle()
      val waitLock = Object()

      while !cancelled do {
        Try(waitLock.synchronized(waitLock.wait())) match
          case Failure(e: InterruptedException) if cancelled =>
            Thread.currentThread().interrupt()
          case _ => ()
      }

  private def getTimestampForPartition(partition: Int) =
    System.currentTimeMillis() - (partition * 50L)

  override def cancel(): Unit =
    cancelled = true
