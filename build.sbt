import sbtrelease.ReleaseStateTransformations._

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / excludeLintKeys      := Set(git.useGitDescribe)

lazy val rootScalaVersion = "3.2.2"

lazy val root = (project in file("."))
  .settings(
    name               := "flink-scala-api",
    scalaVersion       := rootScalaVersion,
    crossScalaVersions := Seq("2.12.15", "2.13.8", rootScalaVersion),
    libraryDependencies ++= Seq(
      "org.apache.flink"        % "flink-streaming-java"    % "1.16.1",
      "org.apache.flink"        % "flink-java"              % "1.16.1",
      "io.findify"             %% "flink-adt"               % "0.6.1",
      "org.scalatest"          %% "scalatest"               % "3.2.12" % Test,
      "org.apache.flink"        % "flink-test-utils"        % "1.16.1" % Test,
      "org.apache.flink"        % "flink-test-utils-junit"  % "1.16.1" % Test,
      "com.github.sbt"          % "junit-interface"         % "0.13.3" % Test,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.7.0"
    ),
    libraryDependencies += {
      if (scalaBinaryVersion.value.startsWith("2")) {
        "org.scala-lang" % "scala-reflect" % scalaVersion.value
      } else {
        "org.scala-lang" %% "scala3-compiler" % scalaVersion.value
      }
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
    git.useGitDescribe := true,
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-language:higherKinds",
      "-language:implicitConversions"
    ),
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
                          Seq[ReleaseStep](inquireVersions, setNextVersion, commitNextVersion, pushChanges)
                        else Seq.empty[ReleaseStep])
  )
  .enablePlugins(GitVersioning)
