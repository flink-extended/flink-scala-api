package org.example

import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

class JobFailed(cause: Exception) extends Exception(cause)

@main def job =
  try {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
      .fromElements(1, 2, 3, 4, 5, 6)
      .filter(_ % 2 == 1)
      .map(i => i * i)
      .print()
    throw new RuntimeException("boom")
    try env.execute()
    catch case e: Exception => throw JobFailed(e)
  } catch
    case e: JobFailed =>
      throw e.getCause
    case e: Throwable =>
      e.printStackTrace()
      // failure in main method, not in the Flink job
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env
        .fromElements("printing stacktrace")
        .print()
      env.execute()
