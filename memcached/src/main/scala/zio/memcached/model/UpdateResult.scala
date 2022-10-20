package zio.memcached.model

object UpdateResult {
  sealed trait UpdateResult

  case object Updated extends UpdateResult

  case object Exists extends UpdateResult

  case object NotFound extends UpdateResult
}
