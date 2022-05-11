ThisBuild / version := "1.15-1"

lazy val root = (project in file("."))
  .settings(
    name               := "flink-scala-api",
    scalaVersion       := "3.1.2",
    crossScalaVersions := Seq("2.12.15", "2.13.8", "3.1.2"),
    libraryDependencies ++= Seq(
      "org.apache.flink"        % "flink-streaming-java"    % "1.15.0",
      "org.apache.flink"        % "flink-java"              % "1.15.0",
      "io.findify"             %% "flink-adt"               % "0.6.0",
      "org.scalatest"          %% "scalatest"               % "3.2.12" % Test,
      "org.apache.flink"        % "flink-test-utils"        % "1.15.0" % Test,
      "org.apache.flink"        % "flink-test-utils-junit"  % "1.15.0" % Test,
      "com.github.sbt"          % "junit-interface"         % "0.13.2" % Test,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.7.0"
    ),
    libraryDependencies += {
      if (scalaBinaryVersion.value.startsWith("2")) {
        "org.scala-lang" % "scala-reflect" % scalaVersion.value
      } else {
        "org.scala-lang" %% "scala3-compiler" % scalaVersion.value
      }
    },
    organization      := "io.findify",
    licenses          := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage          := Some(url("https://github.com/findify/flink-scala-api")),
    publishMavenStyle := true,
    publishTo         := sonatypePublishToBundle.value,
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-language:higherKinds"
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/findify/flink-scala-api"),
        "scm:git@github.com:findify/flink-scala-api.git"
      )
    ),
    developers := List(
      Developer(
        id = "romangrebennikov",
        name = "Roman Grebennikov",
        email = "grv@dfdx.me",
        url = url("https://dfdx.me/")
      )
    )
  )
