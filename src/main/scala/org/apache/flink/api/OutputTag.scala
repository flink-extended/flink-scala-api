package org.apache.flink.api

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.{OutputTag => JOutputTag}

/** An [[OutputTag]] is a typed and named tag to use for tagging side outputs of an operator.
  *
  * Example:
  * {{{
  *   val outputTag = OutputTag[String]("late-data")
  * }}}
  *
  * @tparam T
  *   the type of elements in the side-output stream.
  */
@PublicEvolving
class OutputTag[T: TypeInformation](id: String) extends JOutputTag[T](id, implicitly[TypeInformation[T]])

object OutputTag {
  def apply[T: TypeInformation](id: String): OutputTag[T] = new OutputTag(id)
}
