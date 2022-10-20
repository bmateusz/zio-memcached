package zio.memcached.model

import zio.memcached.MemcachedCommand.validateKey
import zio.memcached.model.MetaArithmeticFlags.MetaArithmeticFlag

import java.nio.charset.StandardCharsets

object MetaArithmeticFlags {

  /**
   * The flags used by the 'ma' command
   */
  sealed trait MetaArithmeticFlag {
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
  case object InterpretKeyAsBase64 extends MetaArithmeticFlag {
    override def flag: String = "b"
  }

  /**
   * compare CAS value when storing item
   */
  case class CompareCasToken(cas: CasUnique) extends MetaArithmeticFlag {
    override def flag: String = s"C${cas.value}"
  }

  /**
   * auto create item on miss with supplied TTL
   */
  case class CreateItemOnMiss(seconds: Long) extends MetaArithmeticFlag {
    override def flag: String = s"N$seconds"
  }

  /**
   * initial value to use if auto created after miss (default 0)
   */
  case class InitialValue(seconds: Long) extends MetaArithmeticFlag {
    override def flag: String = s"J$seconds"
  }

  /**
   * delta to apply (decimal unsigned 64-bit number, default 1)
   */
  case class Delta(value: Int) extends MetaArithmeticFlag {
    override def flag: String = s"D$value"
  }

  /**
   * Time-To-Live for item, see "Expiration" above.
   *
   * @param seconds new ttl
   */
  case class UpdateRemainingTTL(seconds: Long) extends MetaArithmeticFlag {
    override def flag: String = s"T$seconds"
  }

  /**
   * return item TTL remaining in seconds (-1 for unlimited)
   */
  case object ReturnItemTTL extends MetaArithmeticFlag {
    override def flag: String = "t"
  }

  /**
   * return current CAS value if successful
   */
  case object ReturnItemCasToken extends MetaArithmeticFlag {
    override def flag: String = "c"
  }

  /**
   * return key as a token
   *
   * MISSING FROM protocol.txt
   */
  case object ReturnKeyAsToken extends MetaArithmeticFlag {
    override def flag: String = "k"
  }

  /**
   * return item value in <data block>
   */
  case object ReturnItemValue extends MetaArithmeticFlag {
    override def flag: String = "v"
  }

  /**
   * opaque value, consumes a token and copies back with response
   *
   * The O(opaque) token is used by this and other commands to allow easier
   * pipelining of requests while saving bytes on the wire for responses. For
   * example: if pipelining three get commands together, you may not know which
   * response belongs to which without also retrieving the key. If the key is very
   * long this can generate a lot of traffic, especially if the data block is very
   * small. Instead, you can supply an "O" flag for each mg with tokens of "1" "2"
   * and "3", to match up responses to the request.
   *
   * @param token Opaque tokens may be up to 32 bytes in length, and are a string similar to keys.
   *
   */
  case class Opaque private(token: String) extends MetaArithmeticFlag {
    def apply(token: String): Option[Opaque] = Option.when(validateKey(token))(Opaque(token))

    override def flag: String = s"O$token"
  }

  sealed trait MetaSetModeFlag extends MetaArithmeticFlag

  /**
   * Increment mode (default)
   */
  case object ModeIncrement extends MetaSetModeFlag {
    override def flag: String = s"MI"
  }
  /**
   * Decrement mode
   */
  case object ModeDecrement extends MetaSetModeFlag {
    override def flag: String = s"MD"
  }

  def apply(flags: Seq[MetaArithmeticFlag]): MetaArithmeticFlags = new MetaArithmeticFlags(flags)

  val empty = new MetaArithmeticFlags(Seq.empty)

  private val FlagWithValueRegex = """([CNJDTOM])(.+)""".r

  def fromString(string: String): MetaArithmeticFlags =
    if (string.isEmpty) {
      empty
    } else {
      val flags = string
        .split(" ")
        .map {
          case "b" => InterpretKeyAsBase64
          case "t" => ReturnItemTTL
          case "c" => ReturnItemCasToken
          case "k" => ReturnKeyAsToken
          case "v" => ReturnItemValue
          case FlagWithValueRegex(flag, value) =>
            flag match {
              case "C" => CompareCasToken(CasUnique(value.toLong))
              case "N" => CreateItemOnMiss(value.toLong)
              case "J" => InitialValue(value.toLong)
              case "D" => Delta(value.toInt)
              case "T" => UpdateRemainingTTL(value.toLong)
              case "O" => Opaque(value)
              case "M" =>
                value match {
                  case "I" => ModeIncrement
                  case "D" => ModeDecrement
                }
            }
          case flag => throw new IllegalArgumentException(s"Unknown flag $flag")
        }
      new MetaArithmeticFlags(flags.toSeq)
    }
}

class MetaArithmeticFlags(val flags: Seq[MetaArithmeticFlag]) {
  import zio.Chunk
  import zio.memcached.Input.{EmptyChunk, WhitespaceChunk}

  val encoded: Chunk[Byte] =
    flags.foldLeft(EmptyChunk) {
      (acc, flag) => acc ++ WhitespaceChunk ++ flag.flag.getBytes(StandardCharsets.US_ASCII)
    }

  override def toString: String = flags.map(_.flag).mkString(" ")
}
