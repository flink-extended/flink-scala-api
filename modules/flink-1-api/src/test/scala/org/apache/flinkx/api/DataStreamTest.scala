package org.apache.flinkx.api

import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.ParallelIteratorInputFormat
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.graph.{StreamEdge, StreamGraph}
import org.apache.flink.streaming.api.operators._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.runtime.partitioner._
import org.apache.flink.util.Collector
import org.apache.flinkx.api.serializers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataStreamTest extends AnyFlatSpec with Matchers with IntegrationTest {
  import DataStreamTest._

  it should "set operator names" in {
    val source1Operator = env.fromSequence(0, 0).name("testSource1")
    val source1         = source1Operator
    assert("testSource1" == source1Operator.name)

    val dataStream1 = source1
      .map(x => 0L)
      .name("testMap")
    assert("testMap" == dataStream1.name)

    val dataStream2 = env
      .fromSequence(0, 0)
      .name("testSource2")
      .keyBy(x => x)
      .reduce((x, y) => 0L)
      .name("testReduce")
    assert("testReduce" == dataStream2.name)

    val connected = dataStream1
      .connect(dataStream2)
      .flatMap({ (in, out: Collector[(Long, Long)]) => }, { (in, out: Collector[(Long, Long)]) => })
      .name("testCoFlatMap")

    assert("testCoFlatMap" == connected.name)

    val func: (((Long, Long), (Long, Long)) => (Long, Long)) =
      (x: (Long, Long), y: (Long, Long)) => (0L, 0L)

    val windowed = connected
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](10)))
      .reduce(func)

    windowed.name("testWindowReduce")

    assert("testWindowReduce" == windowed.name)

    windowed.print()

    val plan = env.getExecutionPlan

    assert(plan contains "testSource1")
    assert(plan contains "testSource2")
    assert(plan contains "testMap")
    assert(plan contains "testReduce")
    assert(plan contains "testCoFlatMap")
    assert(plan contains "testWindowReduce")
  }

  it should "set operator descriptions" in {
    val dataStream1 = env
      .fromSequence(0, 0)
      .setDescription("this is test source 1")
      .map(x => x)
      .setDescription("this is test map 1")
    val dataStream2 = env
      .fromSequence(0, 0)
      .setDescription("this is test source 2")
      .map(x => x)
      .setDescription("this is test map 2")

    val func: (((Long, Long), (Long, Long)) => (Long, Long)) =
      (x: (Long, Long), y: (Long, Long)) => (0L, 0L)
    dataStream1
      .connect(dataStream2)
      .flatMap({ (in, out: Collector[(Long, Long)]) => }, { (in, out: Collector[(Long, Long)]) => })
      .setDescription("this is test co flat map")
      .windowAll(GlobalWindows.create)
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](10)))
      .reduce(func)
      .setDescription("this is test window reduce")
      .print()
    // test functionality through the operator names in the execution plan
    val plan = env.getExecutionPlan
    assert(plan contains "this is test source 1")
    assert(plan contains "this is test source 2")
    assert(plan contains "this is test map 1")
    assert(plan contains "this is test map 2")
    assert(plan contains "this is test co flat map")
    assert(plan contains "this is test window reduce")
  }

  /** Tests that [[DataStream.keyBy]] and [[DataStream.partitionCustom]] result in different and correct topologies.
    * Does the some for the [[ConnectedStreams]].
    */
  it should "set partitions" in {
    val src1: DataStream[(Long, Long)] = env.fromElements((0L, 0L))
    val src2: DataStream[(Long, Long)] = env.fromElements((0L, 0L))

    val connected = src1.connect(src2)

    val group4 = src1.keyBy(x => x._1)

    val gid4 = createDownStreamId(group4)
    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, gid4)))

    // Testing DataStream partitioning
    val partition4: DataStream[_] = src1.keyBy((x: (Long, Long)) => x._1)

    val pid4 = createDownStreamId(partition4)

    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, pid4)))

    // Testing DataStream custom partitioning
    val longPartitioner: Partitioner[Long] = new Partitioner[Long] {
      override def partition(key: Long, numPartitions: Int): Int = 0
    }

    val customPartition1: DataStream[_] =
      src1.partitionCustom(longPartitioner, _._1)
    val customPartition3: DataStream[_] =
      src1.partitionCustom(longPartitioner, _._2)
    val customPartition4: DataStream[_] =
      src1.partitionCustom(longPartitioner, (x: (Long, Long)) => x._1)

    val cpid1 = createDownStreamId(customPartition1)
    val cpid2 = createDownStreamId(customPartition3)
    val cpid3 = createDownStreamId(customPartition4)
    assert(isCustomPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, cpid1)))
    assert(isCustomPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, cpid2)))
    assert(isCustomPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, cpid3)))

    // Testing ConnectedStreams grouping
    val connectedGroup1: ConnectedStreams[_, _] = connected.keyBy(0, 0)
    val downStreamId1: Integer                  = createDownStreamId(connectedGroup1)

    val connectedGroup2: ConnectedStreams[_, _] = connected.keyBy(Array[Int](0), Array[Int](0))
    val downStreamId2: Integer                  = createDownStreamId(connectedGroup2)

    val connectedGroup3: ConnectedStreams[_, _] = connected.keyBy("_1", "_1")
    val downStreamId3: Integer                  = createDownStreamId(connectedGroup3)

    val connectedGroup4: ConnectedStreams[_, _] =
      connected.keyBy(Array[String]("_1"), Array[String]("_1"))
    val downStreamId4: Integer = createDownStreamId(connectedGroup4)

    val connectedGroup5: ConnectedStreams[_, _] = connected.keyBy(x => x._1, x => x._1)
    val downStreamId5: Integer                  = createDownStreamId(connectedGroup5)

    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, downStreamId1)))
    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, downStreamId1)))

    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, downStreamId2)))
    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, downStreamId2)))

    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, downStreamId3)))
    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, downStreamId3)))

    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, downStreamId4)))
    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, downStreamId4)))

    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, downStreamId5)))
    assert(isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, downStreamId5)))

    // Testing ConnectedStreams partitioning
    val connectedPartition1: ConnectedStreams[_, _] = connected.keyBy(0, 0)
    val connectDownStreamId1: Integer               = createDownStreamId(connectedPartition1)

    val connectedPartition2: ConnectedStreams[_, _] =
      connected.keyBy(Array[Int](0), Array[Int](0))
    val connectDownStreamId2: Integer = createDownStreamId(connectedPartition2)

    val connectedPartition3: ConnectedStreams[_, _] = connected.keyBy("_1", "_1")
    val connectDownStreamId3: Integer               = createDownStreamId(connectedPartition3)

    val connectedPartition4: ConnectedStreams[_, _] =
      connected.keyBy(Array[String]("_1"), Array[String]("_1"))
    val connectDownStreamId4: Integer = createDownStreamId(connectedPartition4)

    val connectedPartition5: ConnectedStreams[_, _] =
      connected.keyBy(x => x._1, x => x._1)
    val connectDownStreamId5: Integer = createDownStreamId(connectedPartition5)

    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, connectDownStreamId1))
    )
    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, connectDownStreamId1))
    )

    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, connectDownStreamId2))
    )
    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, connectDownStreamId2))
    )

    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, connectDownStreamId3))
    )
    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, connectDownStreamId3))
    )

    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, connectDownStreamId4))
    )
    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, connectDownStreamId4))
    )

    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src1.getId, connectDownStreamId5))
    )
    assert(
      isPartitioned(getStreamGraph(env).getStreamEdges(src2.getId, connectDownStreamId5))
    )
  }

  it should "set parallelism" in {
    val parallelism = env.getParallelism

    val src                                = env.fromElements(new Tuple2[Long, Long](0L, 0L))
    val map                                = src.map(x => (0L, 0L))
    val windowed: DataStream[(Long, Long)] = map
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](10)))
      .reduce((x: (Long, Long), y: (Long, Long)) => (0L, 0L))

    windowed.print()
    val sink = map.addSink(x => {})

    assert(1 == getStreamGraph(env).getStreamNode(src.getId).getParallelism)
    assert(parallelism == getStreamGraph(env).getStreamNode(map.getId).getParallelism)
    assert(1 == getStreamGraph(env).getStreamNode(windowed.getId).getParallelism)
    assert(
      parallelism == getStreamGraph(env)
        .getStreamNode(sink.getTransformation.getId)
        .getParallelism
    )

    try {
      src.setParallelism(3)
      fail()
    } catch {
      case success: IllegalArgumentException => {}
    }

    val newParallelism = parallelism + 1
    env.setParallelism(newParallelism)
    // the parallelism does not change since some windowing code takes the parallelism from
    // input operations and that cannot change dynamically
    assert(1 == getStreamGraph(env).getStreamNode(src.getId).getParallelism)
    assert(parallelism == getStreamGraph(env).getStreamNode(map.getId).getParallelism)
    assert(1 == getStreamGraph(env).getStreamNode(windowed.getId).getParallelism)
    assert(
      parallelism == getStreamGraph(env)
        .getStreamNode(sink.getTransformation.getId)
        .getParallelism
    )

    val parallelSource = env.fromSequence(0, 0)
    parallelSource.print()

    assert(newParallelism == getStreamGraph(env).getStreamNode(parallelSource.getId).getParallelism)

    parallelSource.setParallelism(3)
    assert(3 == getStreamGraph(env).getStreamNode(parallelSource.getId).getParallelism)

    map.setParallelism(2)
    assert(2 == getStreamGraph(env).getStreamNode(map.getId).getParallelism)

    sink.setParallelism(4)
    assert(4 == getStreamGraph(env).getStreamNode(sink.getTransformation.getId).getParallelism)
  }

  /** Tests setting the parallelism after a partitioning operation (e.g., broadcast, rescale) should fail.
    */
  it should "fail parallelism after partitioning" in {
    val src = env.fromElements(new Tuple2[Long, Long](0L, 0L))
    val map = src.map(_ => (0L, 0L))

    // This could be replaced with other partitioning operations (e.g., rescale, shuffle, forward),
    // which trigger the setConnectionType() method.
    val broadcastStream = map.broadcast

    an[UnsupportedOperationException] shouldBe thrownBy {
      broadcastStream.setParallelism(1)
    }
  }

  /** Tests whether resource gets set.
    */
  /*
  it should "set resources" in {
    val minResource1: ResourceSpec = new ResourceSpec(1.0, 100)
    val preferredResource1: ResourceSpec = new ResourceSpec(2.0, 200)
    val minResource2: ResourceSpec = new ResourceSpec(1.0, 200)
    val preferredResource2: ResourceSpec = new ResourceSpec(2.0, 300)
    val minResource3: ResourceSpec = new ResourceSpec(1.0, 300)
    val preferredResource3: ResourceSpec = new ResourceSpec(2.0, 400)
    val minResource4: ResourceSpec = new ResourceSpec(1.0, 400)
    val preferredResource4: ResourceSpec = new ResourceSpec(2.0, 500)
    val minResource5: ResourceSpec = new ResourceSpec(1.0, 500)
    val preferredResource5: ResourceSpec = new ResourceSpec(2.0, 600)
    val minResource6: ResourceSpec = new ResourceSpec(1.0, 600)
    val preferredResource6: ResourceSpec = new ResourceSpec(2.0, 700)
    val minResource7: ResourceSpec = new ResourceSpec(1.0, 700)
    val preferredResource7: ResourceSpec = new ResourceSpec(2.0, 800)

    val source1: DataStream[Long] = env.generateSequence(0, 0)
      .resource(minResource1, preferredResource1)
    val map1: DataStream[String] = source1.map(x => "")
      .resource(minResource2, preferredResource2)
    val source2: DataStream[Long] = env.generateSequence(0, 0)
      .resource(minResource3, preferredResource3)
    val map2: DataStream[String] = source2.map(x => "")
      .resource(minResource4, preferredResource4)

    val connected: DataStream[String] = map1.connect(map2)
      .flatMap({ (in, out: Collector[(String)]) => }, { (in, out: Collector[(String)]) => })
      .resource(minResource5, preferredResource5)

    val windowed  = connected
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](5)))
      .reduce((accumulator: String, value: String) => "")
      .resource(minResource6, preferredResource6)

    var sink = windowed.print().resource(minResource7, preferredResource7)

    val plan = env.getExecutionPlan

    assertEquals(minResource1, getStreamGraph(env).getStreamNode(source1.getId).
      getMinResource)
    assertEquals(preferredResource1, getStreamGraph(env).getStreamNode(source1.getId).
      getPreferredResource)
    assertEquals(minResource2, getStreamGraph(env).getStreamNode(map1.getId).
      getMinResource)
    assertEquals(preferredResource2, getStreamGraph(env).getStreamNode(map1.getId).
      getPreferredResource)
    assertEquals(minResource3, getStreamGraph(env).getStreamNode(source2.getId).
      getMinResource)
    assertEquals(preferredResource3, getStreamGraph(env).getStreamNode(source2.getId).
      getPreferredResource)
    assertEquals(minResource4, getStreamGraph(env).getStreamNode(map2.getId).
      getMinResource)
    assertEquals(preferredResource4, getStreamGraph(env).getStreamNode(map2.getId).
      getPreferredResource)
    assertEquals(minResource5, getStreamGraph(env).getStreamNode(connected.getId).
      getMinResource)
    assertEquals(preferredResource5, getStreamGraph(env).getStreamNode(connected.getId).
      getPreferredResource)
    assertEquals(minResource6, getStreamGraph(env).getStreamNode(windowed.getId).
      getMinResource)
    assertEquals(preferredResource6, getStreamGraph(env).getStreamNode(windowed.getId).
      getPreferredResource)
    assertEquals(minResource7, getStreamGraph(env).getStreamNode(
      sink.getPreferredResource.getId).getMinResource)
    assertEquals(preferredResource7, getStreamGraph(env).getStreamNode(
      sink.getPreferredResource.getId).getPreferredResource)
  }*/

  it should "keep type information" in {
    val src1: DataStream[Long] = env.fromSequence(0, 0)
    assert(TypeExtractor.getForClass(classOf[Long]) == src1.dataType)

    val map: DataStream[(Integer, String)] = src1.map(x => null)
    assert(classOf[scala.Tuple2[Integer, String]] == map.dataType.getTypeClass)

    val window: DataStream[String] = map
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](5)))
      .apply((w: GlobalWindow, x: Iterable[(Integer, String)], y: Collector[String]) => {})

    assert(TypeExtractor.getForClass(classOf[String]) == window.dataType)

    val flatten: DataStream[CustomCaseClass] = window
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](5)))
      .aggregate(new AggregateFunction[String, CustomCaseClass, CustomCaseClass] {
        override def createAccumulator(): CustomCaseClass = CustomCaseClass(0, "")

        override def add(value: String, accumulator: CustomCaseClass): CustomCaseClass = ???

        override def getResult(accumulator: CustomCaseClass): CustomCaseClass = ???

        override def merge(a: CustomCaseClass, b: CustomCaseClass): CustomCaseClass = ???
      })

    val typeInfo = implicitly[TypeInformation[CustomCaseClass]]

    typeInfo shouldBe flatten.dataType
  }

  /** Verify that a [[KeyedStream.process(ProcessFunction)]] call is correctly translated to an operator.
    */
  it should "translate keyed stream process function to operator" in {
    val src = env.fromSequence(0, 0)

    val processFunction = new KeyedProcessFunction[Long, Long, Int] {
      override def processElement(
          value: Long,
          ctx: KeyedProcessFunction[Long, Long, Int]#Context,
          out: Collector[Int]
      ): Unit = ???
    }

    val flatMapped = src.keyBy(x => x).process(processFunction)

    assert(processFunction == getFunctionForDataStream(flatMapped))
    assert(getOperatorForDataStream(flatMapped).isInstanceOf[KeyedProcessOperator[_, _, _]])
  }

  /** Verify that a [[KeyedStream.process(KeyedProcessFunction)]] call is correctly translated to an operator.
    */
  it should "translate keyed stream keyed process function to operator" in {
    val src = env.fromSequence(0, 0)

    val keyedProcessFunction = new KeyedProcessFunction[Long, Long, Int] {
      override def processElement(
          value: Long,
          ctx: KeyedProcessFunction[Long, Long, Int]#Context,
          out: Collector[Int]
      ): Unit = ???
    }

    val flatMapped = src.keyBy(x => x).process(keyedProcessFunction)

    assert(keyedProcessFunction == getFunctionForDataStream(flatMapped))
    assert(getOperatorForDataStream(flatMapped).isInstanceOf[KeyedProcessOperator[_, _, _]])
  }

  /** Verify that a [[DataStream.process(ProcessFunction)]] call is correctly translated to an operator.
    */
  it should "translate stream process function to operator" in {
    val src = env.fromSequence(0, 0)

    val processFunction = new ProcessFunction[Long, Int] {
      override def processElement(value: Long, ctx: ProcessFunction[Long, Int]#Context, out: Collector[Int]): Unit = ???
    }

    val flatMapped = src.process(processFunction)

    assert(processFunction == getFunctionForDataStream(flatMapped))
    assert(getOperatorForDataStream(flatMapped).isInstanceOf[ProcessOperator[_, _]])
  }

  it should "create stream graph from operators" in {
    val src = env.fromSequence(0, 0)

    val mapFunction = new MapFunction[Long, Int] {
      override def map(value: Long): Int = 0
    }

    val map = src.map(mapFunction)
    assert(mapFunction == getFunctionForDataStream(map))
    assert(getFunctionForDataStream(map.map(x => 0)).isInstanceOf[MapFunction[_, _]])

    val statefulMap2 =
      src.keyBy(x => x).mapWithState((in, state: Option[Long]) => (in, None.asInstanceOf[Option[Long]]))

    val flatMapFunction = new FlatMapFunction[Long, Int] {
      override def flatMap(value: Long, out: Collector[Int]): Unit = {}
    }

    val flatMap = src.flatMap(flatMapFunction)
    assert(flatMapFunction == getFunctionForDataStream(flatMap))
    assert(
      getFunctionForDataStream(
        flatMap
          .flatMap((x: Int, out: Collector[Int]) => {})
      )
        .isInstanceOf[FlatMapFunction[_, _]]
    )

    val statefulfMap2 =
      src.keyBy(x => x).flatMapWithState((in, state: Option[Long]) => (List(in), None.asInstanceOf[Option[Long]]))

    val filterFunction = new FilterFunction[Int] {
      override def filter(value: Int): Boolean = false
    }

    val unionFilter = map.union(flatMap).filter(filterFunction)
    assert(filterFunction == getFunctionForDataStream(unionFilter))
    assert(
      getFunctionForDataStream(
        map
          .filter((x: Int) => true)
      )
        .isInstanceOf[FilterFunction[_]]
    )

    val statefulFilter2 = src.keyBy(x => x).filterWithState[Long]((in, state: Option[Long]) => (false, None))

    try {
      getStreamGraph(env).getStreamEdges(map.getId, unionFilter.getId)
    } catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }

    try {
      getStreamGraph(env).getStreamEdges(flatMap.getId, unionFilter.getId)
    } catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }

    val connect = map.connect(flatMap)

    val coMapFunction =
      new CoMapFunction[Int, Int, String] {
        override def map1(value: Int): String = ""

        override def map2(value: Int): String = ""
      }
    val coMap = connect.map(coMapFunction)
    assert(coMapFunction == getFunctionForDataStream(coMap))

    try {
      getStreamGraph(env).getStreamEdges(map.getId, coMap.getId)
    } catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }
    try {
      getStreamGraph(env).getStreamEdges(flatMap.getId, coMap.getId)
    } catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }
  }

  it should "assign partitioner" in {
    val src = env.fromSequence(0, 0)

    val broadcast            = src.broadcast
    val broadcastSink        = broadcast.print()
    val broadcastPartitioner = getStreamGraph(env)
      .getStreamEdges(src.getId, broadcastSink.getTransformation.getId)
      .get(0)
      .getPartitioner
    assert(broadcastPartitioner.isInstanceOf[BroadcastPartitioner[_]])

    val shuffle: DataStream[Long] = src.shuffle
    val shuffleSink               = shuffle.print()
    val shufflePartitioner        = getStreamGraph(env)
      .getStreamEdges(src.getId, shuffleSink.getTransformation.getId)
      .get(0)
      .getPartitioner
    assert(shufflePartitioner.isInstanceOf[ShufflePartitioner[_]])

    val forward: DataStream[Long] = src.forward
    val forwardSink               = forward.print()
    val forwardPartitioner        = getStreamGraph(env)
      .getStreamEdges(src.getId, forwardSink.getTransformation.getId)
      .get(0)
      .getPartitioner
    assert(forwardPartitioner.isInstanceOf[ForwardPartitioner[_]])

    val rebalance: DataStream[Long] = src.rebalance
    val rebalanceSink               = rebalance.print()
    val rebalancePartitioner        = getStreamGraph(env)
      .getStreamEdges(src.getId, rebalanceSink.getTransformation.getId)
      .get(0)
      .getPartitioner
    assert(rebalancePartitioner.isInstanceOf[RebalancePartitioner[_]])

    val global: DataStream[Long] = src.global
    val globalSink               = global.print()
    val globalPartitioner        = getStreamGraph(env)
      .getStreamEdges(src.getId, globalSink.getTransformation.getId)
      .get(0)
      .getPartitioner
    assert(globalPartitioner.isInstanceOf[GlobalPartitioner[_]])
  }

  it should "set iterations" in {
    // we need to rebalance before iteration
    val source = env.fromElements(1, 2, 3).map { (t: Int) => t }

    val iterated = source
      .iterate(
        (input: ConnectedStreams[Int, String]) => {
          val head = input.map(i => (i + 1).toString, s => s)
          (head.filter(_ == "2"), head.filter(_ != "2"))
        },
        1000
      )
      .print()

    val iterated2 = source.iterate((input: DataStream[Int]) => (input.map(_ + 1), input.map(_.toString)), 2000)

    val sg = getStreamGraph(env)

    assert(sg.getIterationSourceSinkPairs.size() == 2)
  }

  it should "pass created type information" in {
    env.createInput[Tuple1[Integer]](new ParallelIteratorInputFormat[Tuple1[Integer]](null))
  }

  /////////////////////////////////////////////////////////////
  // Utilities
  /////////////////////////////////////////////////////////////

  private def getFunctionForDataStream(dataStream: DataStream[_]): Function = {
    dataStream.print()
    val operator = getOperatorForDataStream(dataStream)
      .asInstanceOf[AbstractUdfStreamOperator[_, _]]
    operator.getUserFunction.asInstanceOf[Function]
  }

  private def getOperatorForDataStream(dataStream: DataStream[_]): StreamOperator[_] = {
    dataStream.print()
    val streamGraph: StreamGraph = env.getStreamGraph(false)
    streamGraph.getStreamNode(dataStream.getId).getOperator
  }

  /** Returns the StreamGraph without clearing the transformations. */
  private def getStreamGraph(sEnv: StreamExecutionEnvironment): StreamGraph = {
    sEnv.getStreamGraph(false)
  }

  private def isPartitioned(edges: java.util.List[StreamEdge]) = {
    edges.stream.allMatch(_.getPartitioner.isInstanceOf[KeyGroupStreamPartitioner[_, _]])
  }

  private def isCustomPartitioned(edges: java.util.List[StreamEdge]): Boolean = {
    edges.stream.allMatch(_.getPartitioner.isInstanceOf[CustomPartitionerWrapper[_, _]])
  }

  private def createDownStreamId(dataStream: DataStream[_]): Integer = {
    dataStream.print().getTransformation.getId
  }

  private def createDownStreamId(dataStream: ConnectedStreams[_, _]): Integer = {
    val m = dataStream.map(x => 0, x => 0)
    m.print()
    m.getId
  }
}

object DataStreamTest {
  case class CustomCaseClass(id: Int, name: String)
}
