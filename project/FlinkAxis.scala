// Custom axis for Flink major version
case class FlinkAxis(flinkMajor: String) extends sbt.VirtualAxis.WeakAxis {
  override def directorySuffix: String = s"-flink$flinkMajor"
  override def idSuffix: String        = s"-$flinkMajor-api-common"
}

object FlinkAxis {
  val Flink1 = FlinkAxis("1")
  val Flink2 = FlinkAxis("2")
}
