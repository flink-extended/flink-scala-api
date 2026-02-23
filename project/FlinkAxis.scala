// Custom axis for Flink major version
case class FlinkAxis(flinkMajor: String, suffix: String) extends sbt.VirtualAxis.WeakAxis {
  override def directorySuffix: String = s"-flink$flinkMajor"
  override def idSuffix: String        = suffix

  // Took example on sbt.VirtualAxis.ScalaVersionAxis: use only the flinkMajor field for equality
  override def equals(obj: Any): Boolean = obj match {
    case value: AnyRef if this eq value => true
    case o: FlinkAxis                   => this.flinkMajor == o.flinkMajor
    case _                              => false
  }

  override def hashCode: Int = 37 * (17 + "FlinkAxis".hashCode()) + flinkMajor.hashCode()
}

object FlinkAxis {
  val Flink1Common = FlinkAxis("1", "-1-api-common")
  val Flink2Common = FlinkAxis("2", "-2-api-common")
  val Flink1Api = FlinkAxis("1", "")
  val Flink2Api = FlinkAxis("2", "")
}
