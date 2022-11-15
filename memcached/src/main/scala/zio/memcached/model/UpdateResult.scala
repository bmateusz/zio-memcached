package zio.memcached.model

/**
 * Result of the [[zio.memcached.api.Storage.compareAndSwap]] operation.
 */
object UpdateResult {
  sealed trait UpdateResult

  case object Updated extends UpdateResult

  case object Exists extends UpdateResult

  case object NotFound extends UpdateResult
}
