ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion       := "2.13.8"
ThisBuild / crossScalaVersions := Seq("2.13.8", "3.1.2")

lazy val root = (project in file("."))
  .settings(
    name := "flink-scala-api",
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % "1.15.0",
      "org.scalatest"   %% "scalatest"            % "3.2.12" % "test"
    )
  )
