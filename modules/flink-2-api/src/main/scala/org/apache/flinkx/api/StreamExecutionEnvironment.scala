/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flinkx.api

import org.apache.flink.annotation.{Experimental, Internal, Public, PublicEvolving}
import org.apache.flink.api.common.ExecutionConfig.ClosureCleanerLevel
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.io.{FileInputFormat, FilePathFilter, InputFormat}
import org.apache.flink.api.common.operators.SlotSharingGroup
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.{ExecutionConfig, JobExecutionResult, RuntimeExecutionMode}
import org.apache.flink.api.connector.source.lib.NumberSequenceSource
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.core.execution.{JobClient, JobListener}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.util.{SplittableIterator, TernaryBoolean}
import org.apache.flinkx.api.ScalaStreamOps._
import org.apache.flinkx.api.typeinfo.FailFastTypeInfoFactory
import org.slf4j.{Logger, LoggerFactory}

import java.lang.reflect.Type
import java.net.URI
import java.util
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Try

@Public
class StreamExecutionEnvironment(javaEnv: JavaEnv) {

  /** @return the wrapped Java environment */
  def getJavaEnv: JavaEnv = javaEnv

  /** Gets the config object. */
  def getConfig = javaEnv.getConfig

  /** Gets cache files. */
  def getCachedFiles: util.List[tuple.Tuple2[String, DistributedCache.DistributedCacheEntry]] = javaEnv.getCachedFiles

  /** Gets the config JobListeners. */
  @PublicEvolving
  def getJobListeners: util.List[JobListener] = javaEnv.getJobListeners

  /** Sets the parallelism for operations executed through this environment. Setting a parallelism of x here will cause
    * all operators (such as join, map, reduce) to run with x parallel instances. This value can be overridden by
    * specific operations using [[DataStream#setParallelism(int)]].
    */
  def setParallelism(parallelism: Int): Unit = {
    javaEnv.setParallelism(parallelism)
  }

  /** Sets the runtime execution mode for the application (see [[RuntimeExecutionMode]]). This is equivalent to setting
    * the "execution.runtime-mode" in your application's configuration file.
    *
    * We recommend users to NOT use this method but set the "execution.runtime-mode" using the command-line when
    * submitting the application. Keeping the application code configuration-free allows for more flexibility as the
    * same application will be able to be executed in any execution mode.
    *
    * @param executionMode
    *   the desired execution mode.
    * @return
    *   The execution environment of your application.
    */
  @PublicEvolving
  def setRuntimeMode(executionMode: RuntimeExecutionMode): StreamExecutionEnvironment = {
    javaEnv.setRuntimeMode(executionMode)
    this
  }

  /** Sets the maximum degree of parallelism defined for the program. The maximum degree of parallelism specifies the
    * upper limit for dynamic scaling. It also defines the number of key groups used for partitioned state.
    */
  def setMaxParallelism(maxParallelism: Int): Unit = {
    javaEnv.setMaxParallelism(maxParallelism)
  }

  /** Register a slot sharing group with its resource spec.
    *
    * <p>Note that a slot sharing group hints the scheduler that the grouped operators CAN be deployed into a shared
    * slot. There's no guarantee that the scheduler always deploy the grouped operators together. In cases grouped
    * operators are deployed into separate slots, the slot resources will be derived from the specified group
    * requirements.
    *
    * @param slotSharingGroup
    *   which contains name and its resource spec.
    */
  @PublicEvolving
  def registerSlotSharingGroup(slotSharingGroup: SlotSharingGroup): StreamExecutionEnvironment = {
    javaEnv.registerSlotSharingGroup(slotSharingGroup)
    this
  }

  /** Returns the default parallelism for this execution environment. Note that this value can be overridden by
    * individual operations using [[DataStream#setParallelism(int)]]
    */
  def getParallelism = javaEnv.getParallelism

  /** Returns the maximum degree of parallelism defined for the program.
    *
    * The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also defines the number of key
    * groups used for partitioned state.
    */
  def getMaxParallelism: Int = javaEnv.getMaxParallelism

  /** Sets the maximum time frequency (milliseconds) for the flushing of the output buffers. By default the output
    * buffers flush frequently to provide low latency and to aid smooth developer experience. Setting the parameter can
    * result in three logical modes:
    *
    * <ul> <li>A positive integer triggers flushing periodically by that integer</li> <li>0 triggers flushing after
    * every record thus minimizing latency</li> <li>-1 triggers flushing only when the output buffer is full thus
    * maximizing throughput</li> </ul>
    */
  def setBufferTimeout(timeoutMillis: Long): StreamExecutionEnvironment = {
    javaEnv.setBufferTimeout(timeoutMillis)
    this
  }

  /** Gets the default buffer timeout set for this environment */
  def getBufferTimeout: Long = javaEnv.getBufferTimeout

  /** Disables operator chaining for streaming operators. Operator chaining allows non-shuffle operations to be
    * co-located in the same thread fully avoiding serialization and de-serialization.
    */
  @PublicEvolving
  def disableOperatorChaining(): StreamExecutionEnvironment = {
    javaEnv.disableOperatorChaining()
    this
  }

  // ------------------------------------------------------------------------
  //  Checkpointing Settings
  // ------------------------------------------------------------------------

  /** Gets the checkpoint config, which defines values like checkpoint interval, delay between checkpoints, etc.
    */
  def getCheckpointConfig: CheckpointConfig = javaEnv.getCheckpointConfig

  /** Enables checkpointing for the streaming job. The distributed state of the streaming dataflow will be periodically
    * snapshotted. In case of a failure, the streaming dataflow will be restarted from the latest completed checkpoint.
    *
    * The job draws checkpoints periodically, in the given interval. The system uses the given [[CheckpointingMode]] for
    * the checkpointing ("exactly once" vs "at least once"). The state will be stored in the configured state backend.
    *
    * NOTE: Checkpointing iterative streaming dataflows in not properly supported at the moment. For that reason,
    * iterative jobs will not be started if used with enabled checkpointing. To override this mechanism, use the
    * [[enableCheckpointing(long, CheckpointingMode, boolean)]] method.
    *
    * @param interval
    *   Time interval between state checkpoints in milliseconds.
    * @param mode
    *   The checkpointing mode, selecting between "exactly once" and "at least once" guarantees.
    */
  def enableCheckpointing(interval: Long, mode: CheckpointingMode): StreamExecutionEnvironment = {
    javaEnv.enableCheckpointing(interval, mode)
    this
  }

  /** Enables checkpointing for the streaming job. The distributed state of the streaming dataflow will be periodically
    * snapshotted. In case of a failure, the streaming dataflow will be restarted from the latest completed checkpoint.
    *
    * The job draws checkpoints periodically, in the given interval. The program will use
    * [[CheckpointingMode.EXACTLY_ONCE]] mode. The state will be stored in the configured state backend.
    *
    * NOTE: Checkpointing iterative streaming dataflows in not properly supported at the moment. For that reason,
    * iterative jobs will not be started if used with enabled checkpointing. To override this mechanism, use the
    * [[enableCheckpointing(long, CheckpointingMode, boolean)]] method.
    *
    * @param interval
    *   Time interval between state checkpoints in milliseconds.
    */
  def enableCheckpointing(interval: Long): StreamExecutionEnvironment = {
    enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE)
  }

  def getCheckpointingMode = javaEnv.getCheckpointingMode

  /** Enable the change log for current state backend. This change log allows operators to persist state changes in a
    * very fine-grained manner. Currently, the change log only applies to keyed state, so non-keyed operator state and
    * channel state are persisted as usual. The 'state' here refers to 'keyed state'. Details are as follows:
    *
    * Stateful operators write the state changes to that log (logging the state), in addition to applying them to the
    * state tables in RocksDB or the in-mem Hashtable.
    *
    * An operator can acknowledge a checkpoint as soon as the changes in the log have reached the durable checkpoint
    * storage.
    *
    * The state tables are persisted periodically, independent of the checkpoints. We call this the materialization of
    * the state on the checkpoint storage.
    *
    * Once the state is materialized on checkpoint storage, the state changelog can be truncated to the corresponding
    * point.
    *
    * It establish a way to drastically reduce the checkpoint interval for streaming applications across state backends.
    * For more details please check the FLIP-158.
    *
    * If this method is not called explicitly, it means no preference for enabling the change log. Configs for change
    * log enabling will override in different config levels (job/local/cluster).
    *
    * @param enabled
    *   true if enable the change log for state backend explicitly, otherwise disable the change log.
    * @return
    *   This StreamExecutionEnvironment itself, to allow chaining of function calls.
    * @see
    *   #isChangelogStateBackendEnabled()
    */
  @PublicEvolving
  def enableChangelogStateBackend(enabled: Boolean): StreamExecutionEnvironment = {
    javaEnv.enableChangelogStateBackend(enabled)
    this
  }

  /** Gets the enable status of change log for state backend.
    *
    * @return
    *   a [[TernaryBoolean]] for the enable status of change log for state backend. Could be
    *   [[TernaryBoolean#UNDEFINED]] if user never specify this by calling [[enableChangelogStateBackend(boolean)]].
    */
  @PublicEvolving
  def isChangelogStateBackendEnabled: TernaryBoolean = javaEnv.isChangelogStateBackendEnabled

  /** Sets the default savepoint directory, where savepoints will be written to if no is explicitly provided when
    * triggered.
    *
    * @return
    *   This StreamExecutionEnvironment itself, to allow chaining of function calls.
    * @see
    *   #getDefaultSavepointDirectory()
    */
  @PublicEvolving
  def setDefaultSavepointDirectory(savepointDirectory: String): StreamExecutionEnvironment = {
    javaEnv.setDefaultSavepointDirectory(savepointDirectory)
    this
  }

  /** Sets the default savepoint directory, where savepoints will be written to if no is explicitly provided when
    * triggered.
    *
    * @return
    *   This StreamExecutionEnvironment itself, to allow chaining of function calls.
    * @see
    *   #getDefaultSavepointDirectory()
    */
  @PublicEvolving
  def setDefaultSavepointDirectory(savepointDirectory: URI): StreamExecutionEnvironment = {
    javaEnv.setDefaultSavepointDirectory(savepointDirectory)
    this
  }

  /** Sets the default savepoint directory, where savepoints will be written to if no is explicitly provided when
    * triggered.
    *
    * @return
    *   This StreamExecutionEnvironment itself, to allow chaining of function calls.
    * @see
    *   #getDefaultSavepointDirectory()
    */
  @PublicEvolving
  def setDefaultSavepointDirectory(savepointDirectory: Path): StreamExecutionEnvironment = {
    javaEnv.setDefaultSavepointDirectory(savepointDirectory)
    this
  }

  /** Gets the default savepoint directory for this Job.
    *
    * @see
    *   #setDefaultSavepointDirectory(Path)
    */
  @PublicEvolving
  def getDefaultSavepointDirectory: Path = javaEnv.getDefaultSavepointDirectory

  /** Sets all relevant options contained in the [[ReadableConfig]] such as e.g.
    * [[org.apache.flink.streaming.api.environment.StreamPipelineOptions#TIME_CHARACTERISTIC]]. It will reconfigure
    * [[StreamExecutionEnvironment]], [[org.apache.flink.api.common.ExecutionConfig]] and
    * [[org.apache.flink.streaming.api.environment.CheckpointConfig]].
    *
    * It will change the value of a setting only if a corresponding option was set in the `configuration`. If a key is
    * not present, the current value of a field will remain untouched.
    *
    * @param configuration
    *   a configuration to read the values from
    * @param classLoader
    *   a class loader to use when loading classes
    */
  @PublicEvolving
  def configure(configuration: ReadableConfig, classLoader: ClassLoader): Unit = {
    javaEnv.configure(configuration, classLoader)
  }

  /** Sets all relevant options contained in the [[ReadableConfig]] such as e.g.
    * [[org.apache.flink.streaming.api.environment.StreamPipelineOptions#TIME_CHARACTERISTIC]]. It will reconfigure
    * [[StreamExecutionEnvironment]], [[org.apache.flink.api.common.ExecutionConfig]] and
    * [[org.apache.flink.streaming.api.environment.CheckpointConfig]].
    *
    * It will change the value of a setting only if a corresponding option was set in the `configuration`. If a key is
    * not present, the current value of a field will remain untouched.
    *
    * @param configuration
    *   a configuration to read the values from
    */
  @PublicEvolving
  def configure(configuration: ReadableConfig): Unit = {
    javaEnv.configure(configuration)
  }

  // --------------------------------------------------------------------------------------------
  // Data stream creations
  // --------------------------------------------------------------------------------------------

  /** Creates a new data stream that contains a sequence of numbers (longs) and is useful for testing and for cases that
    * just need a stream of N events of any kind.
    *
    * The generated source splits the sequence into as many parallel sub-sequences as there are parallel source readers.
    * Each sub-sequence will be produced in order. If the parallelism is limited to one, the source will produce one
    * sequence in order.
    *
    * This source is always bounded. For very long sequences (for example over the entire domain of long integer
    * values), you may consider executing the application in a streaming manner because of the end bound that is pretty
    * far away.
    *
    * Use [[fromSource(Source,WatermarkStrategy, String)]] together with [[NumberSequenceSource]] if you required more
    * control over the created sources. For example, if you want to set a [[WatermarkStrategy]].
    */
  def fromSequence(from: Long, to: Long): DataStream[Long] = {
    new DataStream[java.lang.Long](javaEnv.fromSequence(from, to))
      .asInstanceOf[DataStream[Long]]
  }

  /** Creates a DataStream that contains the given elements. The elements must all be of the same type.
    *
    * Note that this operation will result in a non-parallel data source, i.e. a data source with a parallelism of one.
    */
  def fromElements[T: TypeInformation](data: T*): DataStream[T] = {
    fromCollection(data)
  }

  /** Creates a DataStream from the given non-empty [[Seq]]. The elements need to be serializable because the framework
    * may move the elements into the cluster if needed.
    *
    * Note that this operation will result in a non-parallel data source, i.e. a data source with a parallelism of one.
    */
  def fromCollection[T: TypeInformation](data: Seq[T]): DataStream[T] = {
    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]

    val collection = data.asJavaCollection
    asScalaStream(javaEnv.fromCollection(collection, typeInfo))
  }

  /** Creates a DataStream from the given [[Iterator]].
    *
    * Note that this operation will result in a non-parallel data source, i.e. a data source with a parallelism of one.
    */
  def fromCollection[T: TypeInformation](data: Iterator[T]): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    asScalaStream(javaEnv.fromCollection(data.asJava, typeInfo))
  }

  /** Creates a DataStream from the given [[SplittableIterator]]. */
  def fromParallelCollection[T: TypeInformation](data: SplittableIterator[T]): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    asScalaStream(javaEnv.fromParallelCollection(data, typeInfo))
  }

  /** Reads the given file with the given input format. The file path should be passed as a URI (e.g.,
    * "file:///some/local/file" or "hdfs://host:port/file/path").
    * @deprecated
    *   Use {@code FileSource#forRecordStreamFormat()/forBulkFileFormat()/forRecordFileFormat() instead}.
    */
  @Deprecated
  def readFile[T: TypeInformation](inputFormat: FileInputFormat[T], filePath: String): DataStream[T] =
    asScalaStream(javaEnv.readFile(inputFormat, filePath))

  /** Reads the contents of the user-specified path based on the given [[FileInputFormat]]. Depending on the provided
    * [[FileProcessingMode]], the source may periodically monitor (every `interval` ms) the path for new data
    * ([[FileProcessingMode.PROCESS_CONTINUOUSLY]]), or process once the data currently in the path and exit
    * ([[FileProcessingMode.PROCESS_ONCE]]). In addition, if the path contains files not to be processed, the user can
    * specify a custom [[FilePathFilter]]. As a default implementation you can use
    * [[FilePathFilter.createDefaultFilter()]].
    *
    * ** NOTES ON CHECKPOINTING: ** If the `watchType` is set to [[FileProcessingMode#PROCESS_ONCE]], the source
    * monitors the path ** once **, creates the [[org.apache.flink.core.fs.FileInputSplit FileInputSplits]] to be
    * processed, forwards them to the downstream [[ContinuousFileReaderOperator readers]] to read the actual data, and
    * exits, without waiting for the readers to finish reading. This implies that no more checkpoint barriers are going
    * to be forwarded after the source exits, thus having no checkpoints after that point.
    *
    * @param inputFormat
    *   The input format used to create the data stream
    * @param filePath
    *   The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
    * @param watchType
    *   The mode in which the source should operate, i.e. monitor path and react to new data, or process once and exit
    * @param interval
    *   In the case of periodic path monitoring, this specifies the interval (in millis) between consecutive path scans
    * @return
    *   The data stream that represents the data read from the given file
    *
    * @deprecated
    *   Use {@code FileSource#forRecordStreamFormat()/forBulkFileFormat()/forRecordFileFormat() instead}.
    */
  @Deprecated
  def readFile[T: TypeInformation](
      inputFormat: FileInputFormat[T],
      filePath: String,
      watchType: FileProcessingMode,
      interval: Long
  ): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    asScalaStream(javaEnv.readFile(inputFormat, filePath, watchType, interval, typeInfo))
  }

  /** Creates a new DataStream that contains the strings received infinitely from socket. Received strings are decoded
    * by the system's default character set. The maximum retry interval is specified in seconds, in case of temporary
    * service outage reconnection is initiated every second.
    */
  @PublicEvolving
  def socketTextStream(hostname: String, port: Int, delimiter: Char = '\n', maxRetry: Long = 0): DataStream[String] =
    asScalaStream(javaEnv.socketTextStream(hostname, port))

  /** Generic method to create an input data stream with a specific input format. Since all data streams need specific
    * information about their types, this method needs to determine the type of the data produced by the input format.
    * It will attempt to determine the data type by reflection, unless the input format implements the
    * ResultTypeQueryable interface.
    */
  @PublicEvolving
  def createInput[T: TypeInformation](inputFormat: InputFormat[T, _]): DataStream[T] =
    if (inputFormat.isInstanceOf[ResultTypeQueryable[_]]) {
      asScalaStream(javaEnv.createInput(inputFormat))
    } else {
      asScalaStream(javaEnv.createInput(inputFormat, implicitly[TypeInformation[T]]))
    }

  /** Create a DataStream using a [[Source]]. */
  @Experimental
  def fromSource[T: TypeInformation](
      source: Source[T, _ <: SourceSplit, _],
      watermarkStrategy: WatermarkStrategy[T],
      sourceName: String
  ): DataStream[T] = {

    val typeInfo = implicitly[TypeInformation[T]]
    asScalaStream(javaEnv.fromSource(source, watermarkStrategy, sourceName, typeInfo))
  }

  /** Triggers the program execution. The environment will execute all parts of the program that have resulted in a
    * "sink" operation. Sink operations are for example printing results or forwarding them to a message queue.
    *
    * The program execution will be logged and displayed with a generated default name.
    *
    * @return
    *   The result of the job execution, containing elapsed time and accumulators.
    */
  def execute(): JobExecutionResult = javaEnv.execute()

  /** Triggers the program execution. The environment will execute all parts of the program that have resulted in a
    * "sink" operation. Sink operations are for example printing results or forwarding them to a message queue.
    *
    * The program execution will be logged and displayed with the provided name.
    *
    * @return
    *   The result of the job execution, containing elapsed time and accumulators.
    */
  def execute(jobName: String): JobExecutionResult = javaEnv.execute(jobName)

  /** Register a [[JobListener]] in this environment. The [[JobListener]] will be notified on specific job status
    * changed.
    */
  @PublicEvolving
  def registerJobListener(jobListener: JobListener): Unit = {
    javaEnv.registerJobListener(jobListener)
  }

  /** Clear all registered [[JobListener]]s. */
  @PublicEvolving def clearJobListeners(): Unit = {
    javaEnv.clearJobListeners()
  }

  /** Triggers the program execution asynchronously. The environment will execute all parts of the program that have
    * resulted in a "sink" operation. Sink operations are for example printing results or forwarding them to a message
    * queue.
    *
    * The program execution will be logged and displayed with a generated default name.
    *
    * <b>ATTENTION:</b> The caller of this method is responsible for managing the lifecycle of the returned
    * [[JobClient]]. This means calling [[JobClient#close()]] at the end of its usage. In other case, there may be
    * resource leaks depending on the JobClient implementation.
    *
    * @return
    *   A [[JobClient]] that can be used to communicate with the submitted job, completed on submission succeeded.
    */
  @PublicEvolving
  def executeAsync(): JobClient = javaEnv.executeAsync()

  /** Triggers the program execution asynchronously. The environment will execute all parts of the program that have
    * resulted in a "sink" operation. Sink operations are for example printing results or forwarding them to a message
    * queue.
    *
    * The program execution will be logged and displayed with the provided name.
    *
    * <b>ATTENTION:</b> The caller of this method is responsible for managing the lifecycle of the returned
    * [[JobClient]]. This means calling [[JobClient#close()]] at the end of its usage. In other case, there may be
    * resource leaks depending on the JobClient implementation.
    *
    * @return
    *   A [[JobClient]] that can be used to communicate with the submitted job, completed on submission succeeded.
    */
  @PublicEvolving
  def executeAsync(jobName: String): JobClient = javaEnv.executeAsync(jobName)

  /** Creates the plan with which the system will execute the program, and returns it as a String using a JSON
    * representation of the execution data flow graph. Note that this needs to be called, before the plan is executed.
    */
  def getExecutionPlan: String = javaEnv.getExecutionPlan

  /** Getter of the [[org.apache.flink.streaming.api.graph.StreamGraph]] of the streaming job. This call clears
    * previously registered [[org.apache.flink.api.dag.Transformation transformations]].
    *
    * @return
    *   The StreamGraph representing the transformations
    */
  @Internal
  def getStreamGraph: StreamGraph = javaEnv.getStreamGraph

  /** Getter of the [[org.apache.flink.streaming.api.graph.StreamGraph]] of the streaming job with the option to clear
    * previously registered [[org.apache.flink.api.dag.Transformation transformations]]. Clearing the transformations
    * allows, for example, to not re-execute the same operations when calling [[execute()]] multiple times.
    *
    * @param clearTransformations
    *   Whether or not to clear previously registered transformations
    * @return
    *   The StreamGraph representing the transformations
    */
  @Internal
  def getStreamGraph(clearTransformations: Boolean): StreamGraph = {
    javaEnv.getStreamGraph(clearTransformations)
  }

  /** Gives read-only access to the underlying configuration of this environment.
    *
    * Note that the returned configuration might not be complete. It only contains options that have initialized the
    * environment or options that are not represented in dedicated configuration classes such as [[ExecutionConfig]] or
    * [[CheckpointConfig]].
    *
    * Use [[configure]] to set options that are specific to this environment.
    */
  @Internal
  def getConfiguration: ReadableConfig = javaEnv.getConfiguration

  /** Getter of the wrapped [[org.apache.flink.streaming.api.environment.StreamExecutionEnvironment]]
    *
    * @return
    *   The encased ExecutionEnvironment
    */
  @Internal
  def getWrappedStreamExecutionEnvironment: JavaEnv = javaEnv

  /** Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning is not disabled in the
    * [[org.apache.flink.api.common.ExecutionConfig]]
    */
  private[flinkx] def scalaClean[F <: AnyRef](f: F): F = {
    if (getConfig.isClosureCleanerEnabled) {
      ClosureCleaner.scalaClean(
        f,
        checkSerializable = true,
        cleanTransitively = getConfig.getClosureCleanerLevel == ClosureCleanerLevel.RECURSIVE
      )
    } else {
      ClosureCleaner.ensureSerializable(f)
    }
    f
  }

  /** Registers a file at the distributed cache under the given name. The file will be accessible from any user-defined
    * function in the (distributed) runtime under a local path. Files may be local files (which will be distributed via
    * BlobServer), or files in a distributed file system. The runtime will copy the files temporarily to a local cache,
    * if needed.
    *
    * The [[org.apache.flink.api.common.functions.RuntimeContext]] can be obtained inside UDFs via
    * [[org.apache.flink.api.common.functions.RichFunction#getRuntimeContext()]] and provides access
    * [[org.apache.flink.api.common.cache.DistributedCache]] via
    * [[org.apache.flink.api.common.functions.RuntimeContext#getDistributedCache()]].
    *
    * @param filePath
    *   The path of the file, as a URI (e.g. "file:///some/path" or "hdfs://host:port/and/path")
    * @param name
    *   The name under which the file is registered.
    */
  def registerCachedFile(filePath: String, name: String): Unit = {
    javaEnv.registerCachedFile(filePath, name)
  }

  /** Registers a file at the distributed cache under the given name. The file will be accessible from any user-defined
    * function in the (distributed) runtime under a local path. Files may be local files (which will be distributed via
    * BlobServer), or files in a distributed file system. The runtime will copy the files temporarily to a local cache,
    * if needed.
    *
    * The [[org.apache.flink.api.common.functions.RuntimeContext]] can be obtained inside UDFs via
    * [[org.apache.flink.api.common.functions.RichFunction#getRuntimeContext()]] and provides access
    * [[org.apache.flink.api.common.cache.DistributedCache]] via
    * [[org.apache.flink.api.common.functions.RuntimeContext#getDistributedCache()]].
    *
    * @param filePath
    *   The path of the file, as a URI (e.g. "file:///some/path" or "hdfs://host:port/and/path")
    * @param name
    *   The name under which the file is registered.
    * @param executable
    *   flag indicating whether the file should be executable
    */
  def registerCachedFile(filePath: String, name: String, executable: Boolean): Unit = {
    javaEnv.registerCachedFile(filePath, name, executable)
  }

  /** Returns whether Unaligned Checkpoints are enabled. */
  def isUnalignedCheckpointsEnabled: Boolean = javaEnv.isUnalignedCheckpointsEnabled

  /** Returns whether Unaligned Checkpoints are force-enabled. */
  def isForceUnalignedCheckpoints: Boolean = javaEnv.isForceUnalignedCheckpoints
}

object StreamExecutionEnvironment {

  @transient lazy val log: Logger = LoggerFactory.getLogger(classOf[StreamExecutionEnvironment])

  /** Sets the default parallelism that will be used for the local execution environment created by
    * [[createLocalEnvironment()]].
    *
    * @param parallelism
    *   The default parallelism to use for local execution.
    */
  @PublicEvolving
  def setDefaultLocalParallelism(parallelism: Int): Unit =
    JavaEnv.setDefaultLocalParallelism(parallelism)

  /** Gets the default parallelism that will be used for the local execution environment created by
    * [[createLocalEnvironment()]].
    */
  @PublicEvolving
  def getDefaultLocalParallelism: Int = JavaEnv.getDefaultLocalParallelism

  // --------------------------------------------------------------------------
  //  context environment
  // --------------------------------------------------------------------------

  /** Creates an execution environment that represents the context in which the program is currently executed. If the
    * program is invoked standalone, this method returns a local execution environment. If the program is invoked from
    * within the command line client to be submitted to a cluster, this method returns the execution environment of this
    * cluster.
    */
  def getExecutionEnvironment: StreamExecutionEnvironment = {
    configureExtra()
    new StreamExecutionEnvironment(JavaEnv.getExecutionEnvironment)
  }

  /** Creates an execution environment that represents the context in which the program is currently executed.
    *
    * @param configuration
    *   Pass a custom configuration into the cluster.
    */
  def getExecutionEnvironment(configuration: Configuration): StreamExecutionEnvironment = {
    configureExtra()
    new StreamExecutionEnvironment(JavaEnv.getExecutionEnvironment(configuration))
  }

  // --------------------------------------------------------------------------
  //  local environment
  // --------------------------------------------------------------------------

  /** Creates a local execution environment. The local execution environment will run the program in a multi-threaded
    * fashion in the same JVM as the environment was created in.
    *
    * This method sets the environment's default parallelism to given parameter, which defaults to the value set via
    * [[setDefaultLocalParallelism(Int)]].
    */
  def createLocalEnvironment(parallelism: Int = JavaEnv.getDefaultLocalParallelism): StreamExecutionEnvironment = {
    configureExtra()
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironment(parallelism))
  }

  /** Creates a local execution environment. The local execution environment will run the program in a multi-threaded
    * fashion in the same JVM as the environment was created in.
    *
    * @param parallelism
    *   The parallelism for the local environment.
    * @param configuration
    *   Pass a custom configuration into the cluster.
    */
  def createLocalEnvironment(parallelism: Int, configuration: Configuration): StreamExecutionEnvironment = {
    configureExtra()
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironment(parallelism, configuration))
  }

  /** Creates a [[StreamExecutionEnvironment]] for local program execution that also starts the web monitoring UI.
    *
    * The local execution environment will run the program in a multi-threaded fashion in the same JVM as the
    * environment was created in. It will use the parallelism specified in the parameter.
    *
    * If the configuration key 'rest.port' was set in the configuration, that particular port will be used for the web
    * UI. Otherwise, the default port (8081) will be used.
    *
    * @param config
    *   optional config for the local execution
    * @return
    *   The created StreamExecutionEnvironment
    */
  @PublicEvolving
  def createLocalEnvironmentWithWebUI(config: Configuration = null): StreamExecutionEnvironment = {
    configureExtra()
    val conf: Configuration = if (config == null) new Configuration() else config
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironmentWithWebUI(conf))
  }

  // --------------------------------------------------------------------------
  //  remote environment
  // --------------------------------------------------------------------------

  /** Creates a remote execution environment. The remote environment sends (parts of) the program to a cluster for
    * execution. Note that all file paths used in the program must be accessible from the cluster. The execution will
    * use the cluster's default parallelism, unless the parallelism is set explicitly via
    * [[StreamExecutionEnvironment.setParallelism()]].
    *
    * @param host
    *   The host name or address of the master (JobManager), where the program should be executed.
    * @param port
    *   The port of the master (JobManager), where the program should be executed.
    * @param jarFiles
    *   The JAR files with code that needs to be shipped to the cluster. If the program uses user-defined functions,
    *   user-defined input formats, or any libraries, those must be provided in the JAR files.
    */
  def createRemoteEnvironment(host: String, port: Int, jarFiles: String*): StreamExecutionEnvironment = {
    configureExtra()
    new StreamExecutionEnvironment(JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*))
  }

  /** Creates a remote execution environment. The remote environment sends (parts of) the program to a cluster for
    * execution. Note that all file paths used in the program must be accessible from the cluster. The execution will
    * use the specified parallelism.
    *
    * @param host
    *   The host name or address of the master (JobManager), where the program should be executed.
    * @param port
    *   The port of the master (JobManager), where the program should be executed.
    * @param parallelism
    *   The parallelism to use during the execution.
    * @param jarFiles
    *   The JAR files with code that needs to be shipped to the cluster. If the program uses user-defined functions,
    *   user-defined input formats, or any libraries, those must be provided in the JAR files.
    */
  def createRemoteEnvironment(
      host: String,
      port: Int,
      parallelism: Int,
      jarFiles: String*
  ): StreamExecutionEnvironment = {
    configureExtra()
    val javaEnv = JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*)
    javaEnv.setParallelism(parallelism)
    new StreamExecutionEnvironment(javaEnv)
  }

  /** Creates a remote execution environment. The remote environment sends (parts of) the program to a cluster for
    * execution. Note that all file paths used in the program must be accessible from the cluster. The execution will
    * use the specified parallelism.
    *
    * @param host
    *   The host name or address of the master (JobManager), where the program should be executed.
    * @param port
    *   The port of the master (JobManager), where the program should be executed.
    * @param config
    *   Pass a custom configuration into the cluster.
    * @param jarFiles
    *   The JAR files with code that needs to be shipped to the cluster. If the program uses user-defined functions,
    *   user-defined input formats, or any libraries, those must be provided in the JAR files.
    */
  def createRemoteEnvironment(
      host: String,
      port: Int,
      config: Configuration,
      jarFiles: String*
  ): StreamExecutionEnvironment = {
    configureExtra()
    val javaEnv = JavaEnv.createRemoteEnvironment(host, port, config, jarFiles: _*)
    new StreamExecutionEnvironment(javaEnv)
  }

  /** Add extra configuration:
    *   - register type info factories to fail-fast on Scala type resolution with Class
    */
  private def configureExtra(): Unit = {
    if (!isFailFastOnScalaTypeResolutionWithClassConfigured && !isFailFastOnScalaTypeResolutionWithClassDisabled) {
      isFailFastOnScalaTypeResolutionWithClassConfigured = true
      try {
        val typeExtractorClass    = Class.forName("org.apache.flink.api.java.typeutils.TypeExtractor")
        val registerFactoryMethod = typeExtractorClass.getMethod("registerFactory", classOf[Type], classOf[Class[_]])
        registerFactoryMethod.invoke(null, classOf[Product], classOf[FailFastTypeInfoFactory])
        registerFactoryMethod.invoke(null, classOf[Option[_]], classOf[FailFastTypeInfoFactory])
        registerFactoryMethod.invoke(null, classOf[Either[_, _]], classOf[FailFastTypeInfoFactory])
        registerFactoryMethod.invoke(null, classOf[Iterable[_]], classOf[FailFastTypeInfoFactory])
      } catch {
        case t: Throwable =>
          log.info(
            s"Unable to activate 'fail-fast on Scala type resolution with Class' feature: available from Flink 1.19: $t"
          )
      }
    }
  }

  private var isFailFastOnScalaTypeResolutionWithClassConfigured: Boolean = false

  private lazy val isFailFastOnScalaTypeResolutionWithClassDisabled: Boolean =
    sys.env
      .get("DISABLE_FAIL_FAST_ON_SCALA_TYPE_RESOLUTION_WITH_CLASS")
      .exists(v => Try(v.toBoolean).getOrElse(false))

}
