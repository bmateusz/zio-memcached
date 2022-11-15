package zio.memcached.model

/**
 * Base trait for all meta flags.
 */
trait MetaFlagBase {
  def flag: String

  override def toString: String = flag
}
