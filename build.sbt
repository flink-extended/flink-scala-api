import sbtrelease.ReleaseStateTransformations.*
import xerial.sbt.Sonatype.sonatypeCentralHost

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / excludeLintKeys      := Set(git.useGitDescribe, crossScalaVersions)

lazy val rootScalaVersion = "3.3.6"
lazy val crossVersions    = Seq("2.13.16", rootScalaVersion)
lazy val flinkVersion1    = System.getProperty("flinkVersion1", "1.20.1")
lazy val flinkVersion2    = System.getProperty("flinkVersion2", "2.0.0")

lazy val root = (project in file("."))
  .aggregate(`scala-api-common`, `flink-1-api`, `flink-2-api`, `examples`)
  .settings(commonSettings)
  .settings(
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := crossVersions,
    publish / skip     := true
  )

lazy val commonSettings = Seq(
  scalaVersion       := rootScalaVersion,
  crossScalaVersions := crossVersions,
  libraryDependencies ++= {
    if (scalaBinaryVersion.value.startsWith("2")) {
      Seq(
        "com.softwaremill.magnolia1_2" %% "magnolia"      % "1.1.10",
        "org.scala-lang"                % "scala-reflect" % scalaVersion.value % Provided
      )
    } else {
      Seq(
        "com.softwaremill.magnolia1_3" %% "magnolia"        % "1.3.16",
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
  organization           := "org.flinkextended",
  description            := "Community-maintained fork of official Apache Flink Scala API",
  licenses               := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage               := Some(url("https://github.com/flink-extended/flink-scala-api")),
  sonatypeCredentialHost := sonatypeCentralHost,
  sonatypeRepository     := "https://s01.oss.sonatype.org/service/local",
  publishMavenStyle      := true,
  publishTo              := sonatypePublishToBundle.value,
  pgpPassphrase          := scala.util.Properties.propOrNone("gpg.passphrase").map(_.toCharArray),
  git.useGitDescribe     := true,
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
  },
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/flink-extended/flink-scala-api"),
      "scm:git@github.com:flink-extended/flink-scala-api.git"
    )
  ),
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
  releaseProcess := Seq.empty[ReleaseStep],
  releaseProcess ++= (if (sys.env.contains("RELEASE_VERSION_BUMP"))
                        Seq[ReleaseStep](
                          checkSnapshotDependencies,
                          inquireVersions,
                          setReleaseVersion,
                          commitReleaseVersion,
                          tagRelease,
                          releaseStepCommandAndRemaining("+publishSigned"),
                          releaseStepCommand("sonatypeBundleRelease")
                        )
                      else Seq.empty[ReleaseStep]),
  releaseProcess ++= (if (sys.env.contains("RELEASE_PUBLISH"))
                        Seq[ReleaseStep](
                          inquireVersions,
                          setNextVersion,
                          commitNextVersion,
                          pushChanges
                        )
                      else Seq.empty[ReleaseStep])
)

lazy val `scala-api-common` = (project in file("modules/flink-common-api"))
  .settings(commonSettings)
  .settings(
    name               := "flink-scala-api-common",
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := crossVersions,
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % flinkVersion1 % Provided,
      "org.scalatest"   %% "scalatest"            % "3.2.19"      % Test,
      "ch.qos.logback"   % "logback-classic"      % "1.5.17"      % Test
    )
  )

def flinkDependencies(flinkVersion: String) =
  Seq(
    "org.apache.flink"  % "flink-streaming-java"        % flinkVersion % Provided,
    "org.apache.flink"  % "flink-table-api-java-bridge" % flinkVersion % Provided,
    "org.apache.flink"  % "flink-test-utils"            % flinkVersion % Test,
    ("org.apache.flink" % "flink-streaming-java"        % flinkVersion % Test).classifier("tests"),
    "org.typelevel"    %% "cats-core"                   % "2.13.0"     % Test,
    "org.scalatest"    %% "scalatest"                   % "3.2.19"     % Test,
    "ch.qos.logback"    % "logback-classic"             % "1.5.17"     % Test
  )

lazy val `flink-1-api` = (project in file("modules/flink-1-api"))
  .dependsOn(`scala-api-common`)
  .settings(commonSettings)
  .settings(
    name               := "flink-scala-api-1",
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := crossVersions,
    libraryDependencies ++= (flinkDependencies(
      flinkVersion1
    ) :+ "org.apache.flink" % "flink-java" % flinkVersion1 % Provided)
  )

lazy val `flink-2-api` = (project in file("modules/flink-2-api"))
  .dependsOn(`scala-api-common`)
  .settings(commonSettings)
  .settings(
    name               := "flink-scala-api-2",
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := crossVersions,
    libraryDependencies ++= flinkDependencies(flinkVersion2)
  )

lazy val docs = project
  .in(file("modules/docs")) // important: it must not be docs/
  .settings(
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := crossVersions,
    mdocIn             := new File("README.md"),
    publish / skip     := true,
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % flinkVersion1
    )
  )
  .dependsOn(`flink-1-api`)
  .enablePlugins(MdocPlugin)

val flinkMajorAndMinorVersion =
  flinkVersion1.split("\\.").toList.take(2).mkString(".")

lazy val `examples` = (project in file("modules/examples"))
  .dependsOn(`flink-1-api`, `scala-api-common`)
  .settings(
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := Seq(rootScalaVersion),
    Test / fork        := true,
    publish / skip     := true,
    // Release process for the `examples` is not needed
    releaseProcess := Seq.empty[ReleaseStep],
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-runtime-web"         % flinkVersion1 % Provided,
      "org.apache.flink" % "flink-clients"             % flinkVersion1 % Provided,
      "org.apache.flink" % "flink-state-processor-api" % flinkVersion1 % Provided,
      // Kafka Connector version is weird and to be set here manually
      "org.apache.flink" % "flink-connector-kafka"      % s"3.4.0-1.20" % Provided,
      "org.apache.flink" % "flink-connector-files"      % flinkVersion1 % Provided,
      "org.apache.flink" % "flink-table-runtime"        % flinkVersion1 % Provided,
      "org.apache.flink" % "flink-table-planner-loader" % flinkVersion1 % Provided,
      "io.bullet"       %% "borer-core"                 % "1.16.1"      % Provided,
      "ch.qos.logback"   % "logback-classic"            % "1.4.14"      % Provided,
      "org.apache.flink" % "flink-test-utils"           % flinkVersion1 % Test,
      "org.apache.flink" % "flink-streaming-java"       % flinkVersion1 % Test classifier "tests",
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
