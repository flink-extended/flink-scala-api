package org.apache.flinkx

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer

package object api {

  /** @deprecated
    *   Use [[auto]] for a direct replacement, or [[semiauto]] for semi-auto derivation.
    */
  @deprecated(since = "2.2.0", message = "Use `auto` for a direct replacement or `semiauto` for semi-auto derivation.")
  // Implicits priority order (linearization): serializers > Implicits > AutoImplicits
  object serializers extends AutoImplicits with Implicits

  /** Basic type has an arity of 1. See [[BasicTypeInfo#getArity()]] */
  private[api] val BasicTypeArity: Int = 1

  /** Basic type has 1 field. See [[BasicTypeInfo#getTotalFields()]] */
  private[api] val BasicTypeTotalFields: Int = 1

  /** Documentation of [[TypeInformation#getTotalFields()]] states the total number of fields must be at least 1. */
  private[api] val MinimumTotalFields: Int = 1

  /** Documentation of [[TypeSerializer#getLength()]] states data type with variable length must return `-1`. */
  private[api] val VariableLengthDataType: Int = -1

  /** Mark a null value in the stream of serialized data. It is validly used only when these conditions are met:
    *   - Used in both serialize and deserialize methods of the serializer.
    *   - The range of actual data doesn't include [[Int.MinValue]], i.e., the size of a collection can be only >= 0.
    *   - The actual data written in the stream is an Int: the first data to deserialize must be an Int for both null
    *     and non-null cases.
    *
    * If one of these conditions is not met, consider using another marker or wrap your serializer into a
    * [[NullableSerializer]].
    */
  private[api] val NullMarker: Int = Int.MinValue

}
