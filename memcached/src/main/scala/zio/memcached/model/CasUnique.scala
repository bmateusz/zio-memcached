package zio.memcached.model

/**
 * A "compare and swap token" which can be passed to [[zio.memcached.api.Storage.compareAndSwap]] to update the value if
 * it has not changed.
 *
 * @param value
 *   The numeric value of the token
 */
case class CasUnique(value: Long) extends AnyVal
