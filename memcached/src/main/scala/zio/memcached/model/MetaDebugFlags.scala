package zio.memcached.model

import zio.memcached.model.MetaDebugFlags.MetaDebugFlag

import java.nio.charset.StandardCharsets

object MetaDebugFlags {

  /**
   * The flags used by the 'mg' command
   */
  sealed trait MetaDebugFlag {
    def flag: String
  }

  /**
   * interpret key as base64 encoded binary value
   *
   * This flag instructs memcached to run a base64 decoder on <key> before looking
   * it up. This allows storing and fetching of binary packed keys, so long as they
   * are sent to memcached in base64 encoding.
   *
   * If 'b' flag is sent in the response, and a key is returned via 'k', this
   * signals to the client that the key is base64 encoded binary.
   */
  case object InterpretKeyAsBase64 extends MetaDebugFlag {
    override def flag: String = "b"
  }

  def apply(flags: Seq[MetaDebugFlag]): MetaDebugFlags = new MetaDebugFlags(flags)

  val empty = new MetaDebugFlags(Seq.empty)

  def fromString(string: String): MetaDebugFlags =
    if (string.isEmpty) {
      empty
    } else {
      val flags = string
        .split(" ")
        .map {
          case "b" => InterpretKeyAsBase64
          case flag => throw new IllegalArgumentException(s"Unknown flag $flag")
        }
      new MetaDebugFlags(flags.toSeq)
    }
}

class MetaDebugFlags(val flags: Seq[MetaDebugFlag]) {

  import zio.Chunk
  import zio.memcached.Input.{EmptyChunk, WhitespaceChunk}

  val encoded: Chunk[Byte] =
    flags.foldLeft(EmptyChunk) {
      (acc, flag) => acc ++ WhitespaceChunk ++ flag.flag.getBytes(StandardCharsets.US_ASCII)
    }

  override def toString: String = flags.map(_.flag).mkString(" ")
}
