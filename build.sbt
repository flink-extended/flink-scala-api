ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion       := "3.1.2"
ThisBuild / crossScalaVersions := Seq("2.13.8", "3.1.2")

lazy val root = (project in file("."))
  .settings(
    name := "flink-scala-api",
    resolvers ++= Seq(
      ("maven snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/")
    ),
    scalaVersion := "3.1.2",
    libraryDependencies ++= Seq(
      "org.apache.flink"        % "flink-streaming-java"    % "1.15.0",
      "org.apache.flink"        % "flink-java"              % "1.15.0",
      "io.findify"             %% "flink-adt"               % "0.6.0-SNAPSHOT",
      "org.scalatest"          %% "scalatest"               % "3.2.12" % Test,
      "org.apache.flink"        % "flink-test-utils"        % "1.15.0" % Test,
      "org.apache.flink"        % "flink-test-utils-junit"  % "1.15.0" % Test,
      "com.github.sbt"          % "junit-interface"         % "0.13.2" % Test,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.7.0"
    )
  )
