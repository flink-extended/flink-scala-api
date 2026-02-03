package org.apache.flinkx.api

/** Provides automatic derivation of TypeInformation for Scala types.
  *
  * Import this object to enable automatic type-information derivation for ADT (case classes, sealed traits) and have
  * access to implicitly available type-information for Scala types, collections, Java types, etc.
  *
  * ==Usage==
  *
  * Simply import `org.apache.flinkx.api.auto._` to enable automatic TypeInformation resolution:
  *
  * {{{
  * import org.apache.flinkx.api.auto._
  *
  * case class User(id: String, age: Int)
  *
  * // TypeInformation is automatically derived
  * val env = StreamExecutionEnvironment.getExecutionEnvironment
  * env.fromElements(User("alice", 30), User("bob", 25))
  * }}}
  *
  * @see
  *   [[semiauto]] for explicit/manual derivation
  * @see
  *   [[AutoImplicits.deriveTypeInformation]] for the automatic derivation method
  */
// Implicits priority order (linearization): auto > Implicits > AutoImplicits. deriveTypeInformation implicit
// method is declared in AutoImplicits to have a lower priority than implicits in Implicits
trait auto extends AutoImplicits with Implicits

object auto extends auto
