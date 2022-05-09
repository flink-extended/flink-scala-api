ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion       := "2.12.15"
ThisBuild / crossScalaVersions := Seq("2.13.8", "3.1.2")

lazy val root = (project in file("."))
  .settings(
    name := "flink-scala-api",
    resolvers ++= Seq(
      ("maven snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/")
    ),
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java"   % "1.15.0",
      "org.apache.flink" % "flink-java"             % "1.15.0",
      "org.scalatest"   %% "scalatest"              % "3.2.12" % Test,
      "io.findify"      %% "flink-adt"              % "0.5.0"  % Test,
      "org.apache.flink" % "flink-test-utils"       % "1.15.0" % Test,
      "org.apache.flink" % "flink-test-utils-junit" % "1.15.0" % Test,
      "com.github.sbt"   % "junit-interface"        % "0.13.2" % Test
    )
  )
