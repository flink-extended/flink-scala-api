package org.apache.flinkx.api

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flinkx.api.semiauto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinedStreamsTest extends AnyFlatSpec with Matchers with IntegrationTest {
  private val dataStream1 = env.fromElements("a1", "a2", "a3")
  private val dataStream2 = env.fromElements("a1", "a2")
  private val keySelector = (s: String) => s
  private val tsAssigner  = TumblingEventTimeWindows.of(Time.milliseconds(1))

  it should "set allowed lateness" in {
    val lateness = Time.milliseconds(42)

    val withLateness = dataStream1
      .join(dataStream2)
      .where(keySelector)
      .equalTo(keySelector)
      .window(tsAssigner)
      .allowedLateness(lateness)

    withLateness.allowedLateness.toMilliseconds shouldBe lateness.toMilliseconds
  }
}
