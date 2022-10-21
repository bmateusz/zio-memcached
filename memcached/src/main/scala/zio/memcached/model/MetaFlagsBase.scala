package zio.memcached.model

import java.nio.charset.StandardCharsets
import zio.Chunk
import zio.memcached.Input.{EmptyChunk, WhitespaceChunk}

abstract class MetaFlagsBase[T <: MetaFlagBase] {
  def flags: Seq[T]

  val encoded: Chunk[Byte] =
    flags.foldLeft(EmptyChunk) { (acc, flag) =>
      acc ++ WhitespaceChunk ++ flag.flag.getBytes(StandardCharsets.US_ASCII)
    }
}
