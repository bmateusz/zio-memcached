package zio.memcached.model

import zio.memcached.MemcachedCommand.isValidKey
import zio.memcached.model.MetaDeleteFlags.MetaDeleteFlag

object MetaDeleteFlags {

  /**
   * The flags used by the 'md' command
   */
  sealed trait MetaDeleteFlag extends MetaFlagBase

  /**
   * interpret key as base64 encoded binary value
   *
   * This flag instructs memcached to run a base64 decoder on <key> before looking it up. This allows storing and
   * fetching of binary packed keys, so long as they are sent to memcached in base64 encoding.
   *
   * If 'b' flag is sent in the response, and a key is returned via 'k', this signals to the client that the key is
   * base64 encoded binary.
   */
  case object InterpretKeyAsBase64 extends MetaDeleteFlag {
    override def flag: String = "b"
  }

  /**
   * compare CAS value when deleting item
   */
  case class CompareCasToken(cas: CasUnique) extends MetaDeleteFlag {
    override def flag: String = s"C${cas.value}"
  }

  /**
   * invalidate. mark as stale, bumps CAS.
   */
  case object Invalidate extends MetaDeleteFlag {
    override def flag: String = "I"
  }

  /**
   * return key as a token
   */
  case object ReturnKeyAsToken extends MetaDeleteFlag {
    override def flag: String = "k"
  }

  /**
   * opaque value, consumes a token and copies back with response
   *
   * The O(opaque) token is used by this and other commands to allow easier pipelining of requests while saving bytes on
   * the wire for responses. For example: if pipelining three get commands together, you may not know which response
   * belongs to which without also retrieving the key. If the key is very long this can generate a lot of traffic,
   * especially if the data block is very small. Instead, you can supply an "O" flag for each mg with tokens of "1" "2"
   * and "3", to match up responses to the request.
   *
   * @param token
   *   Opaque tokens may be up to 32 bytes in length, and are a string similar to keys.
   */
  case class Opaque private (token: String) extends MetaDeleteFlag {
    override def flag: String = s"O$token"
  }

  object Opaque {
    def apply(token: String): Opaque =
      if (isValidKey(token)) new Opaque(token) else throw new IllegalArgumentException("Invalid token for opaque flag")
  }

  /**
   * updates TTL, only when paired with the 'I' flag
   *
   * @param seconds
   *   new ttl
   */
  case class UpdateRemainingTTL(seconds: Long) extends MetaDeleteFlag {
    override def flag: String = s"T$seconds"
  }

  def apply(flags: Seq[MetaDeleteFlag]): MetaDeleteFlags = new MetaDeleteFlags(flags)

  val empty = new MetaDeleteFlags(Seq.empty)

  private val FlagWithValueRegex = """([COT])(.+)""".r

  def fromString(string: String): MetaDeleteFlags =
    if (string.isEmpty) {
      empty
    } else {
      val flags = string
        .split(" ")
        .map {
          case FlagWithValueRegex(flag, value) =>
            flag match {
              case "C" => CompareCasToken(CasUnique(value.toLong))
              case "O" => Opaque(value)
              case "T" => UpdateRemainingTTL(value.toLong)
            }
          case flag =>
            flag match {
              case "b" => InterpretKeyAsBase64
              case "I" => Invalidate
              case "k" => ReturnKeyAsToken
              case _   => throw new IllegalArgumentException(s"Unknown flag $flag")
            }
        }
      new MetaDeleteFlags(flags.toSeq)
    }
}

class MetaDeleteFlags(override val flags: Seq[MetaDeleteFlag]) extends MetaFlagsBase[MetaDeleteFlag]
