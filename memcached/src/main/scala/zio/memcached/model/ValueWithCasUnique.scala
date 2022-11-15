package zio.memcached.model

/**
 * A value with a CAS (compare-and-swap) unique identifier.
 *
 * @param value
 *   The value
 * @param casUnique
 *   The unique "compare and swap token" which can be passed to [[zio.memcached.api.Storage.compareAndSwap]] to update
 *   the value if it has not changed.
 * @tparam T
 *   Type of the value
 */
class ValueWithCasUnique[T](val value: T, val casUnique: CasUnique) {
  override def toString: String = s"ValueWithCasUnique($value, $casUnique)"
}
