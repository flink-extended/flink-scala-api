Global / onChangedBuildSource := ReloadOnSourceChanges
Global / excludeLintKeys      := Set(crossScalaVersions)

lazy val rootScalaVersion = "3.3.7"
lazy val crossVersions    = Seq("2.13.18", rootScalaVersion)
lazy val flinkVersion1    = System.getProperty("flinkVersion1", "1.20.2")
lazy val flinkVersion2    = System.getProperty("flinkVersion2", "2.0.0")

ThisBuild / publishTo := sonatypePublishToBundle.value

inThisBuild(
  List(
    organization := "com.github.sbt",
    homepage     := Some(url("https://github.com/sbt/sbt-ci-release")),
    // Alternatively License.Apache2 see https://github.com/sbt/librarymanagement/blob/develop/core/src/main/scala/sbt/librarymanagement/License.scala
    licenses   := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        id = "romangrebennikov",
        name = "Roman Grebennikov",
        email = "grv@dfdx.me",
        url = url("https://dfdx.me/")
      ),
      Developer(
        id = "novakov-alexey",
        name = "Alexey Novakov",
        email = "novakov.alex@gmail.com",
        url = url("https://novakov-alexey.github.io/")
      )
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/flink-extended/flink-scala-api"),
        "scm:git@github.com:flink-extended/flink-scala-api.git"
      )
    ),
    organization           := "org.flinkextended",
    description            := "Community-maintained fork of official Apache Flink Scala API",
    licenses               := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage               := Some(url("https://github.com/flink-extended/flink-scala-api")),
    sonatypeCredentialHost := "central.sonatype.com"
  )
)

lazy val `flink-scala-api` = (project in file("."))
  .aggregate(
    `flink`.projectRefs ++
      `flink-1-api`.projectRefs ++
      `flink-2-api`.projectRefs ++
      `examples`.projectRefs: _*
  )
  .settings(commonSettings)
  .settings(
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := crossVersions,
    publish / skip     := true
  )

lazy val commonSettings = Seq(
  libraryDependencies ++= {
    if (scalaBinaryVersion.value.startsWith("2")) {
      Seq(
        "com.softwaremill.magnolia1_2" %% "magnolia"      % "1.1.10",
        "org.scala-lang"                % "scala-reflect" % scalaVersion.value % Provided
      )
    } else {
      Seq(
        "com.softwaremill.magnolia1_3" %% "magnolia"        % "1.3.18",
        "org.scala-lang"               %% "scala3-compiler" % scalaVersion.value % Provided
      )
    }
  },
  // some IT tests won't work without running in forked JVM
  Test / fork := true,
  // Need to isolate macro usage to version-specific folders.
  Compile / unmanagedSourceDirectories += {
    val dir              = (Compile / scalaSource).value.getPath
    val Some((major, _)) = CrossVersion.partialVersion(scalaVersion.value)
    file(s"$dir-$major")
  },
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-language:higherKinds",
    "-language:implicitConversions"
  ), // Need extra leniency on how much we can inline during typeinfo derivation.
  scalacOptions ++= {
    if (scalaVersion.value.startsWith("3")) {
      Seq("-Xmax-inlines", "128")
    } else {
      Nil
    }
  }
)

def flinkDependencies(flinkVersion: String) =
  Seq(
    "org.apache.flink"  % "flink-streaming-java"        % flinkVersion % Provided,
    "org.apache.flink"  % "flink-table-api-java-bridge" % flinkVersion % Provided,
    "org.apache.flink"  % "flink-test-utils"            % flinkVersion % Test,
    ("org.apache.flink" % "flink-streaming-java"        % flinkVersion % Test).classifier("tests"),
    "org.typelevel"    %% "cats-core"                   % "2.13.0"     % Test,
    "org.scalatest"    %% "scalatest"                   % "3.2.19"     % Test,
    "ch.qos.logback"    % "logback-classic"             % "1.5.32"     % Test
  )

// val has to be named `flink` in order to generate `flink-1-api-common` and `flink-2-api-common` project ids
lazy val `flink` = (projectMatrix in file("modules/flink-common-api"))
  .settings(commonSettings)
  .customRow(
    scalaVersions = crossVersions,
    axisValues = Seq(FlinkAxis.Flink1Common, VirtualAxis.jvm),
    settings = Seq(
      name := "flink-scala-api-common-1",
      libraryDependencies ++= flinkDependencies(flinkVersion1)
    )
  )
  .customRow(
    scalaVersions = crossVersions,
    axisValues = Seq(FlinkAxis.Flink2Common, VirtualAxis.jvm),
    settings = Seq(
      name := "flink-scala-api-common-2",
      libraryDependencies ++= flinkDependencies(flinkVersion2)
    )
  )

lazy val `flink-1-api` = (projectMatrix in file("modules/flink-1-api"))
  .dependsOn(`flink`)
  .settings(commonSettings)
  .customRow(
    scalaVersions = crossVersions,
    axisValues = Seq(FlinkAxis.Flink1Api, VirtualAxis.jvm),
    settings = Seq(
      name := "flink-scala-api-1",
      libraryDependencies ++= (flinkDependencies(flinkVersion1) :+
        "org.apache.flink" % "flink-java" % flinkVersion1 % Provided)
    )
  )

lazy val `flink-2-api` = (projectMatrix in file("modules/flink-2-api"))
  .dependsOn(`flink`)
  .settings(commonSettings)
  .customRow(
    scalaVersions = crossVersions,
    axisValues = Seq(FlinkAxis.Flink2Api, VirtualAxis.jvm),
    settings = Seq(
      name := "flink-scala-api-2",
      libraryDependencies ++= flinkDependencies(flinkVersion2)
    )
  )

lazy val docs = (projectMatrix in file("modules/docs")) // important: it must not be docs/
  .dependsOn(`flink-1-api`)
  .jvmPlatform(crossVersions)
  .settings(
    mdocIn         := new File("README.md"),
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % flinkVersion1
    )
  )
  .enablePlugins(MdocPlugin)

val flinkMajorAndMinorVersion =
  flinkVersion1.split("\\.").toList.take(2).mkString(".")

lazy val `examples` = (projectMatrix in file("modules/examples"))
  .dependsOn(`flink-1-api`)
  .jvmPlatform(Seq(rootScalaVersion))
  .settings(
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := Seq(rootScalaVersion),
    Test / fork        := true,
    publish / skip     := true,
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-runtime-web"         % flinkVersion1 % Provided,
      "org.apache.flink" % "flink-clients"             % flinkVersion1 % Provided,
      "org.apache.flink" % "flink-state-processor-api" % flinkVersion1 % Provided,
      // Kafka Connector version is weird and to be set here manually
      "org.apache.flink" % "flink-connector-kafka"      % s"3.4.0-1.20" % Provided,
      "org.apache.flink" % "flink-connector-files"      % flinkVersion1 % Provided,
      "org.apache.flink" % "flink-table-runtime"        % flinkVersion1 % Provided,
      "org.apache.flink" % "flink-table-planner-loader" % flinkVersion1 % Provided,
      "io.bullet"       %% "borer-core"                 % "1.16.2"      % Provided,
      "ch.qos.logback"   % "logback-classic"            % "1.4.14"      % Provided,
      "org.apache.flink" % "flink-test-utils"           % flinkVersion1 % Test,
      "org.apache.flink" % "flink-streaming-java"       % flinkVersion1 % Test classifier "tests",
      "org.typelevel"   %% "cats-core"                  % "2.13.0"      % Test,
      "org.scalatest"   %% "scalatest"                  % "3.2.15"      % Test
    ),
    Compile / run := Defaults
      .runTask(
        Compile / fullClasspath,
        Compile / run / mainClass,
        Compile / run / runner
      )
      .evaluated,
    Compile / run / fork := true
  )
  .enablePlugins(ProtobufPlugin)
