# Scala 2.13/3.x API for Apache Flink

[![CI Status](https://github.com/flink-extended/flink-scala-api/workflows/CI/badge.svg)](https://github.com/flinkextended/flink-scala-api/actions)
[![Maven Central](https://img.shields.io/maven-central/v/org.flinkextended/flink-scala-api-2_3)](https://search.maven.org/artifact/org.flinkextended/flink-scala-api-2_3)   
[![License: Apache 2](https://img.shields.io/badge/License-Apache2-green.svg)](https://opensource.org/licenses/Apache-2.0)
![Last commit](https://img.shields.io/github/last-commit/flink-extended/flink-scala-api)
![Last release](https://img.shields.io/github/release/flink-extended/flink-scala-api)

This project is a community-maintained fork of official Apache Flink Scala API, cross-built for scala 2.13 and 3.x.

## Documentation

The documentation lives under [`api-docs/`](api-docs):

- [Migration](api-docs/migration.md) — migrating an existing project to `flink-scala-api`
- [Getting Started](api-docs/getting-started.md) — installation, dependency management, supported Flink versions
- [Examples](api-docs/examples.md) — a tour of the runnable code examples
- [Differences with the Official Flink Scala API](api-docs/differences.md) — the magnolia-based serialization framework and other departures
- [Interaction with Flink's type system](api-docs/type-system.md) — `TypeInformation` derivation, null handling, Java types, type mapping, ordering, schema evolution
- [Converting `RowData` to case classes](api-docs/rowdata.md) — `RowDataConverter` (Scala 3)
- [Compile times](api-docs/compile-times.md) — keeping derivation-heavy builds fast
- [Feature Flags](api-docs/feature-flags.md) — toggles and environment variables

## Release

Add new Git Tag and push to remote. Then watch GitHub Release Pipeline which publishes new artifacts to Sonatype.

## License

This project is using parts of the Apache Flink codebase, so the whole project
is licensed under an [Apache 2.0](LICENSE.md) software license.
