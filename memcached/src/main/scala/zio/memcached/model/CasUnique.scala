package zio.memcached.model

class CasUnique(val value: Long) extends AnyVal {
  override def toString: String = s"CasUnique($value)"
}

object CasUnique {
  def apply(value: Long): CasUnique = new CasUnique(value)
}
