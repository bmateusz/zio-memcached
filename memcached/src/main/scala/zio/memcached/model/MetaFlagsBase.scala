package zio.memcached.model

/**
 * Base class for all meta flags collection.
 */
abstract class MetaFlagsBase[T <: MetaFlagBase] {
  def flags: Seq[T]

  val encoded: String = flags.mkString(" ", " ", "")

  def contains[A <: T](flag: A): Boolean = flags.contains(flag)

  def collectFirst[A <: T](pf: PartialFunction[T, A]): Option[A] = flags.collectFirst(pf)
}
