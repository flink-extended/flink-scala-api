package org.example

import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator
import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.configuration.Configuration

import java.io.File
import java.time.Duration
import java.util.function.Predicate

class MyDefaultFileFilter extends Predicate[Path]:
  override def test(path: Path): Boolean =
    print(s"S3 FILE PATH: $path")

    val fileName = path.getName
    println(s", name: $fileName")

    fileName.headOption match
      case Some(first) =>
        first != '.' && first != '_' && !fileName.startsWith("GRP")
      case None => false

@main def filterFiles =
  val currentDirectory = File(".").getCanonicalPath
  val inputBasePath = Path(s"$currentDirectory/input-table")
  val fileSourceBuilder =
    FileSource.forRecordStreamFormat(
      TextLineInputFormat(),
      inputBasePath
    )

  val fileSource = fileSourceBuilder
    .monitorContinuously(Duration.ofSeconds(2))
    .setFileEnumerator(() =>
      NonSplittingRecursiveEnumerator(MyDefaultFileFilter())
    )
    .build()
  val env =
    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(Configuration())

  env
    .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "csvs")
    .map((_, 1))
    .keyBy(_ => "count")
    .sum(1)
    .map { case (w, c) =>
      println(s"count: $c")
    }

  env.execute("Filter Files")
