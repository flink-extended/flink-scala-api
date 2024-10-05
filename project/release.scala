import sbtrelease.ReleasePlugin._
import sbtrelease.Version
import sbtrelease.versionFormatError
import sbt.AutoPlugin
import sbt._
import Keys._

object ReleaseProcess extends AutoPlugin {
  import sbtrelease.ReleasePlugin.autoImport._

  def cutFlinkPrefix(v: String): String =
    if (v.contains("_")) v.split("_").tail.mkString else v

  def releaseSettings(flinkVersion: String): Seq[Setting[_]] = Seq(
    releaseVersion := { ver =>
      Version(cutFlinkPrefix(ver))
        .map(_.withoutQualifier.unapply)
        .map(v => flinkVersion + "_" + v)
        .getOrElse(versionFormatError(ver))
    },
    releaseNextVersion := { ver =>
      Version(cutFlinkPrefix(ver))
        .map(_.bump(releaseVersionBump.value).asSnapshot.unapply)
        .getOrElse(versionFormatError(ver))
    }
  )
}
