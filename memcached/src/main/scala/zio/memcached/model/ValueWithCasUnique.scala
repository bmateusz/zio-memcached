package zio.memcached.model

class ValueWithCasUnique[T](val value: T, val casUnique: CasUnique) {
  override def toString: String = s"ValueWithCasUnique($value, $casUnique)"
}
