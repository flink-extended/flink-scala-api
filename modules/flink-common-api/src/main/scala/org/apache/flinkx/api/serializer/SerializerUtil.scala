package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{
  CompositeTypeSerializerUtil,
  TypeSerializer,
  TypeSerializerSchemaCompatibility,
  TypeSerializerSnapshot
}

private[serializer] object SerializerUtil {

  /** Resolves the compatibility of a serializer from the compatibility of the serializers it nests.
    *
    * @param registeredSnapshots
    *   the snapshots of the serializers nested in the serializer registered by the current code
    * @param restoredSnapshots
    *   the snapshots of the serializers nested in the restored serializer, as the savepoint holds them
    * @param restoreSerializer
    *   builds the serializer from the reconfigured nested serializers, given in the order of `registeredSnapshots`
    * @tparam T
    *   the type of the serialized data
    */
  def resolveNestedSchemaCompatibility[T](
      registeredSnapshots: Array[TypeSerializerSnapshot[_]],
      restoredSnapshots: Array[TypeSerializerSnapshot[_]],
      restoreSerializer: Array[TypeSerializer[_]] => TypeSerializer[T]
  ): TypeSerializerSchemaCompatibility[T] =
    if (registeredSnapshots.length != restoredSnapshots.length) {
      TypeSerializerSchemaCompatibility.incompatible()
    } else {
      val nested =
        CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult[T](registeredSnapshots, restoredSnapshots)
      if (nested.isCompatibleWithReconfiguredSerializer) {
        TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
          restoreSerializer(nested.getNestedSerializers)
        )
      } else {
        nested.getFinalResult
      }
    }

}
