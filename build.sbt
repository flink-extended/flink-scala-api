import sbtrelease.ReleaseStateTransformations.*

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / excludeLintKeys      := Set(git.useGitDescribe)

lazy val rootScalaVersion = "3.3.3"
lazy val flinkVersion     = System.getProperty("flinkVersion", "1.16.3")

lazy val root = (project in file("."))
  .settings(ReleaseProcess.releaseSettings(flinkVersion) *)
  .settings(
    name               := "flink-scala-api",
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := Seq("2.12.19", "2.13.14", rootScalaVersion),
    libraryDependencies ++= Seq(
      "org.apache.flink"  % "flink-streaming-java" % flinkVersion,
      "org.apache.flink"  % "flink-java"           % flinkVersion,
      "org.apache.flink"  % "flink-test-utils"     % flinkVersion % Test,
      ("org.apache.flink" % "flink-streaming-java" % flinkVersion % Test).classifier("tests"),
      "org.typelevel"    %% "cats-core"            % "2.12.0"     % Test,
      "org.scalatest"    %% "scalatest"            % "3.2.18"     % Test,
      "ch.qos.logback"    % "logback-classic"      % "1.5.6"      % Test
    ),
    libraryDependencies ++= {
      if (scalaBinaryVersion.value.startsWith("2")) {
        Seq(
          "com.softwaremill.magnolia1_2" %% "magnolia"      % "1.1.10",
          "org.scala-lang"                % "scala-reflect" % scalaVersion.value
        )
      } else {
        Seq(
          "com.softwaremill.magnolia1_3" %% "magnolia"        % "1.3.7",
          "org.scala-lang"               %% "scala3-compiler" % scalaVersion.value
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
    organization := "org.flinkextended",
    description  := "Community-maintained fork of official Apache Flink Scala API",
    licenses     := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage     := Some(url("https://github.com/flink-extended/flink-scala-api")),    
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
                        else Seq.empty[ReleaseStep]),
    mdocIn := new File("README.md")
  )
  .enablePlugins(MdocPlugin)
