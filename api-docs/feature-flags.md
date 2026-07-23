# Feature Flags

## Disable fail-fast on Scala type resolution with Class

From [1.2.3 release](https://github.com/flink-extended/flink-scala-api/releases/tag/v1.20.0_1.2.3), a check is done to prevent misusage of Scala type resolution with `Class` which may lead to silently fallback to generic Kryo serializers.

You can disable this check with the `DISABLE_FAIL_FAST_ON_SCALA_TYPE_RESOLUTION_WITH_CLASS` environment variable set to `true`.
