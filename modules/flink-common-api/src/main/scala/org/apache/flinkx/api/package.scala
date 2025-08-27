package org.apache.flinkx

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

package object api {

  /** Basic type has an arity of 1. See [[BasicTypeInfo#getArity()]] */
  private[api] val BasicTypeArity: Int = 1

  /** Basic type has 1 field. See [[BasicTypeInfo#getTotalFields()]] */
  private[api] val BasicTypeTotalFields: Int = 1

  /** Documentation of [[TypeInformation#getTotalFields()]] states the total number of fields must be at least 1. */
  private[api] val MinimumTotalFields: Int = 1

}
