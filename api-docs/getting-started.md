# Getting Started

## Usage 

`flink-scala-api` is released to Maven-central for 2.13 and 3 Scala version. In the same time, we support and release Flink 1.x and 2.x builds of this library. 
- flink-scala-api-1 - is for Flink 1.x
- flink-scala-api-2 - is for Flink 2.x
  
For SBT, add this snippet to `build.sbt`:
```scala
// for Flink 2.x
libraryDependencies += "org.flinkextended" %% "flink-scala-api-2" % "<put latest release tag>"
// OR for Flink 1.x
libraryDependencies += "org.flinkextended" %% "flink-scala-api-1" % "<put latest release tag>"
```

## For Ammonite

```scala
import $ivy.`org.flinkextended::flink-scala-api-1:2.2.0`
// you might need flink-client too in order to run in the REPL
import $ivy.`org.apache.flink:flink-clients:1.20.1`
```

## For Scala 2.12

If you want first to migrate to org.flinkextended:flink-scala-api staying on Scala 2.12, you can use the last build for Scala 2.12:

```scala
libraryDependencies += "org.flinkextended" %% "flink-scala-api" % "1.18.1_1.2.0"
// or
"org.flinkextended" %% "flink-scala-api" % "1.19.1_1.2.0"
// or
"org.flinkextended" %% "flink-scala-api" % "1.20.0_1.2.0"
```

Build for Scala 2.12 is no longer published.

## Dependencies management
When including the Scala API in a fat JAR or adding it to the __flink/lib__ folder, users should ensure that the appropriate Scala API dependencies are included
into flink classpath.
In the case of Scala versions, these dependencies should be:

### For Scala 2.13

- [scala-reflect](https://mvnrepository.com/artifact/org.scala-lang/scala-reflect)
- [magnolia](https://mvnrepository.com/artifact/com.softwaremill.magnolia1_2/magnolia_2.13)

### For Scala 3
- [magnolia](https://mvnrepository.com/artifact/com.softwaremill.magnolia1_3/magnolia)

## SBT Project Template

If you want to create new project easily check this __Giter8 template__ out: [novakov-alexey/flink-scala-api.g8](https://github.com/novakov-alexey/flink-scala-api.g8)

## Supported Flink versions

Three latest of Apache Flink 1.x and one Flink 2.y versions are supported. 
See excact supported versions in the local [ci.yml](../.github/workflows/ci.yml#L16) file.

We suggest to remove the official `flink-scala` and `flink-streaming-scala` deprecated dependencies altogether to simplify the migration and do not to mix two flavors of API in the same project. `flink-scala` dependency is embedding Scala version 2.12.7:
- If you keep them, in order to use the Scala version of your choice, remove `scala` package from `classloader.parent-first-patterns.default` Flink's configuration property:
```diff
- classloader.parent-first-patterns.default: java.;scala.;org.apache.flink.;com.esotericsoftware.kryo;org.apache.hadoop.;javax.annotation.;org.xml;javax.xml;org.apache.xerces;org.w3c;org.rocksdb.;org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback
+ classloader.parent-first-patterns.default: java.;org.apache.flink.;com.esotericsoftware.kryo;org.apache.hadoop.;javax.annotation.;org.xml;javax.xml;org.apache.xerces;org.w3c;org.rocksdb.;org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback
```
- If you choose to remove them, we recommend to test your application with Kryo explicitly disabled (Flink property `pipeline.generic-types: false`), see details in [Interaction with Flink's type system](type-system.md).
