package org.apache.flinkx.api.function.util

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.functions.{AbstractRichFunction, Function, OpenContext, RuntimeContext}

/** Wrapper of [[Function]].
  *
  * Created to replace `org.apache.flink.api.java.operators.translation.WrappingFunction` in Flink 1.x that was moved to
  * `org.apache.flink.api.common.functions.WrappingFunction` in Flink 2.x making it not cross-compatible.
  *
  * @tparam T
  *   the type of the wrapped function.
  * @param wrappedFunction
  *   the underlying function to be wrapped.
  */
class WrappingFunction[T <: Function](val wrappedFunction: T) extends AbstractRichFunction {

  override def open(openContext: OpenContext): Unit = {
    FunctionUtils.openFunction(wrappedFunction, openContext)
  }

  override def close(): Unit = {
    FunctionUtils.closeFunction(wrappedFunction)
  }

  override def setRuntimeContext(runtimeContext: RuntimeContext): Unit = {
    super.setRuntimeContext(runtimeContext)
    FunctionUtils.setFunctionRuntimeContext(wrappedFunction, runtimeContext)
  }

}
