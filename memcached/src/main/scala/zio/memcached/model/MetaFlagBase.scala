package zio.memcached.model

trait MetaFlagBase {
  def flag: String

  override def toString: String = flag
}
