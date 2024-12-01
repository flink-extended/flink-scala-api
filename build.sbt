import sbtrelease.ReleaseStateTransformations.*

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / excludeLintKeys      := Set(git.useGitDescribe)

lazy val rootScalaVersion = "3.3.4"
lazy val crossVersions    = Seq("2.13.15", rootScalaVersion)
lazy val flinkVersion     = System.getProperty("flinkVersion", "1.18.1")

lazy val root = (project in file("."))
  .aggregate(`scala-api`, `examples`)
  .settings(
    scalaVersion   := rootScalaVersion,
    publish / skip := true
  )

lazy val `scala-api` = (project in file("modules/scala-api"))
  .settings(ReleaseProcess.releaseSettings(flinkVersion) *)
  .settings(
    name               := "flink-scala-api",
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := crossVersions,
    libraryDependencies ++= Seq(
      "org.apache.flink"  % "flink-streaming-java" % flinkVersion % Provided,
      "org.apache.flink"  % "flink-java"           % flinkVersion % Provided,
      "org.apache.flink"  % "flink-test-utils"     % flinkVersion % Test,
      ("org.apache.flink" % "flink-streaming-java" % flinkVersion % Test).classifier("tests"),
      "org.typelevel"    %% "cats-core"            % "2.12.0"     % Test,
      "org.scalatest"    %% "scalatest"            % "3.2.19"     % Test,
      "ch.qos.logback"    % "logback-classic"      % "1.5.12"     % Test
    ),
    libraryDependencies ++= {
      if (scalaBinaryVersion.value.startsWith("2")) {
        Seq(
          "com.softwaremill.magnolia1_2" %% "magnolia"      % "1.1.10",
          "org.scala-lang"                % "scala-reflect" % scalaVersion.value % Provided
        )
      } else {
        Seq(
          "com.softwaremill.magnolia1_3" %% "magnolia"        % "1.3.8",
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
    sonatypeCredentialHost := "s01.oss.sonatype.org",
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

lazy val docs = project // new documentation project
  .in(file("modules/docs")) // important: it must not be docs/
  .settings(
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := crossVersions,
    mdocIn             := new File("README.md"),
    publish / skip     := true,
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % flinkVersion
    )
  )
  .dependsOn(`scala-api`)
  .enablePlugins(MdocPlugin)

val flinkMajorAndMinorVersion =
  flinkVersion.split("\\.").toList.take(2).mkString(".")

lazy val `examples` = (project in file("modules/examples"))
  .settings(
    scalaVersion   := rootScalaVersion,
    Test / fork    := true,
    publish / skip := true,
    releaseProcess := Seq.empty[ReleaseStep], // Release for example is not needed
    libraryDependencies ++= Seq(
      "org.flinkextended" %% "flink-scala-api"            % "1.20.0_1.2.1",
      "org.apache.flink"   % "flink-runtime-web"          % "1.20.0"     % Provided,
      "org.apache.flink"   % "flink-clients"              % "1.20.0"     % Provided,
      "org.apache.flink"   % "flink-state-processor-api"  % "1.20.0"     % Provided,
      "org.apache.flink"   % "flink-connector-kafka"      % "3.0.2-1.18" % Provided,
      "org.apache.flink"   % "flink-connector-files"      % "1.20.0"     % Provided,
      "org.apache.flink"   % "flink-table-runtime"        % "1.20.0"     % Provided,
      "org.apache.flink"   % "flink-table-planner-loader" % "1.20.0"     % Provided,
      "io.bullet"         %% "borer-core"                 % "1.14.1"     % Provided,
      "ch.qos.logback"     % "logback-classic"            % "1.4.14"     % Provided,
      "org.apache.flink"   % "flink-test-utils"           % "1.20.0"     % Test,
      "org.apache.flink"   % "flink-streaming-java"       % "1.20.0"     % Test classifier "tests",
      "org.scalatest"     %% "scalatest"                  % "3.2.15"     % Test
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
