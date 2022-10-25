package zio.memcached.model

import zio.Chunk
import zio.memcached.Input.{EmptyChunk, WhitespaceChunk}

import java.nio.charset.StandardCharsets

abstract class MetaFlagsBase[T <: MetaFlagBase] {
  def flags: Seq[T]

  val encoded: Chunk[Byte] =
    flags.foldLeft(EmptyChunk) { (acc, flag) =>
      acc ++ WhitespaceChunk ++ flag.flag.getBytes(StandardCharsets.US_ASCII)
    }

  def contains[A <: T](flag: A): Boolean = flags.contains(flag)

  def collectFirst[A <: T](pf: PartialFunction[T, A]): Option[A] = flags.collectFirst(pf)
}
