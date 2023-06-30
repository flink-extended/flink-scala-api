import sbtrelease.ReleaseStateTransformations._
import ReleaseProcess._

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / excludeLintKeys      := Set(git.useGitDescribe)

lazy val rootScalaVersion = "3.2.2"
lazy val flinkVersion     = System.getProperty("flinkVersion", "1.16.2")

lazy val root = (project in file("."))
  .settings(ReleaseProcess.releaseSettings(flinkVersion): _*)
  .settings(
    name               := "flink-scala-api",
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := Seq("2.12.17", "2.13.11", rootScalaVersion),
    libraryDependencies ++= Seq(
      "org.apache.flink"  % "flink-streaming-java"   % flinkVersion,
      "org.apache.flink"  % "flink-java"             % flinkVersion,
      "org.apache.flink"  % "flink-test-utils"       % flinkVersion % Test,
      "org.apache.flink"  % "flink-test-utils-junit" % flinkVersion % Test,
      ("org.apache.flink" % "flink-streaming-java"   % flinkVersion % Test).classifier("tests"),
      "org.scalatest"    %% "scalatest"              % "3.2.12"     % Test,
      "com.github.sbt"    % "junit-interface"        % "0.13.3"     % Test,
      "org.typelevel"    %% "cats-core"              % "2.7.0"      % Test
    ),
    libraryDependencies ++= {
      if (scalaBinaryVersion.value.startsWith("2")) {
        Seq(
          "com.softwaremill.magnolia1_2" %% "magnolia"      % "1.1.2",
          "org.scala-lang"                % "scala-reflect" % scalaVersion.value
        )
      } else {
        Seq(
          "com.softwaremill.magnolia1_3" %% "magnolia"        % "1.1.1",
          "org.scala-lang"               %% "scala3-compiler" % scalaVersion.value
        )
      }
    },
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
    credentials += Credentials(
      "Sonatype Nexus Repository Manager",
      "oss.sonatype.org",
      "(Sonatype user name)",
      "(Sonatype password)"
    ),
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
    publishMavenStyle  := true,
    publishTo          := sonatypePublishToBundle.value,
    pgpPassphrase      := scala.util.Properties.propOrNone("gpg.passphrase").map(_.toCharArray),
    git.useGitDescribe := true,
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
  .enablePlugins(GitVersioning, MdocPlugin)
