package org.apache.flinkx.api

/** @deprecated
 *   Extend [[auto]] for direct replacement, or [[semiauto]] for semi-auto derivation.
 */
@deprecated(since = "2.2.1", message = "Extend `auto` for direct replacement or `semiauto` for semi-auto derivation.")
// Implicits priority order (linearization): serializers > Implicits > AutoImplicits
trait serializers extends AutoImplicits with Implicits

/** @deprecated
 *   Use [[auto]] for direct replacement, or [[semiauto]] for semi-auto derivation.
 */
@deprecated(since = "2.2.0", message = "Use `auto` for direct replacement or `semiauto` for semi-auto derivation.")
object serializers extends serializers
