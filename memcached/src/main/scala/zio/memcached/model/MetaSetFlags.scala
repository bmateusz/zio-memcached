/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.memcached.model

import zio.memcached.MemcachedCommand.isValidKey
import zio.memcached.model.MetaSetFlags.MetaSetFlag

object MetaSetFlags {

  /**
   * The flags used by the 'ms' command
   */
  sealed trait MetaSetFlag extends MetaFlagBase

  /**
   * interpret key as base64 encoded binary value
   *
   * This flag instructs memcached to run a base64 decoder on <key> before looking it up. This allows storing and
   * fetching of binary packed keys, so long as they are sent to memcached in base64 encoding.
   *
   * If 'b' flag is sent in the response, and a key is returned via 'k', this signals to the client that the key is
   * base64 encoded binary.
   */
  case object InterpretKeyAsBase64 extends MetaSetFlag {
    override def flag: String = "b"
  }

  /**
   * return CAS value if successfully stored
   */
  case object ReturnItemCasUnique extends MetaSetFlag {
    override def flag: String = "c"
  }

  /**
   * compare CAS value when storing item
   */
  case class CompareCasToken(cas: CasUnique) extends MetaSetFlag {
    override def flag: String = s"C${cas.value}"
  }

  /**
   * set client flags to token (32 bit unsigned numeric)
   */
  case class SetClientFlags(value: Int) extends MetaSetFlag {
    override def flag: String = s"F$value"
  }

  /**
   * invalidate. set-to-invalid if supplied CAS is older than item's CAS
   */
  case object InvalidateIfCasIsOlder extends MetaSetFlag {
    override def flag: String = "I"
  }

  /**
   * return key as a token
   */
  case object ReturnKeyAsToken extends MetaSetFlag {
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
  case class Opaque private (token: String) extends MetaSetFlag {
    override def flag: String = s"O$token"
  }

  object Opaque {
    def apply(token: String): Opaque =
      if (isValidKey(token)) new Opaque(token) else throw new IllegalArgumentException("Invalid token for opaque flag")
  }

  /**
   * Time-To-Live for item, see "Expiration" above.
   *
   * @param seconds
   *   new ttl
   */
  case class UpdateRemainingTTL(seconds: Long) extends MetaSetFlag {
    override def flag: String = s"T$seconds"
  }

  sealed trait MetaSetModeFlag extends MetaSetFlag

  /**
   * mode switch to change behavior to add
   */
  case object ModeAdd extends MetaSetModeFlag {
    override def flag: String = s"ME"
  }

  /**
   * mode switch to change behavior to append
   */
  case object ModeAppend extends MetaSetModeFlag {
    override def flag: String = s"MA"
  }

  /**
   * mode switch to change behavior to prepend
   */
  case object ModePrepend extends MetaSetModeFlag {
    override def flag: String = s"MP"
  }

  /**
   * mode switch to change behavior to replace
   */
  case object ModeReplace extends MetaSetModeFlag {
    override def flag: String = s"MR"
  }

  /**
   * mode switch to change behavior to set.
   *
   * The default mode, added for completeness.
   */
  case object ModeSet extends MetaSetModeFlag {
    override def flag: String = s"MS"
  }

  def apply(flags: Seq[MetaSetFlag]): MetaSetFlags = new MetaSetFlags(flags)

  val empty = new MetaSetFlags(Seq.empty)

  private val FlagWithValueRegex = """([CFOT])(.+)""".r

  /**
   * Create a new [[MetaSetFlags]] from a string of flags
   *
   * @param string
   *   the string of flags
   * @return
   *   a [[MetaSetFlags]] object
   */
  def fromString(string: String): MetaSetFlags =
    if (string.isEmpty) {
      empty
    } else {
      val flags = string
        .split(" ")
        .map {
          case FlagWithValueRegex(flag, value) =>
            flag match {
              case "C" => CompareCasToken(CasUnique(value.toLong))
              case "F" => SetClientFlags(value.toInt)
              case "O" => Opaque(value)
              case "T" => UpdateRemainingTTL(value.toLong)
            }
          case flag =>
            flag match {
              case "b"  => InterpretKeyAsBase64
              case "c"  => ReturnItemCasUnique
              case "I"  => InvalidateIfCasIsOlder
              case "k"  => ReturnKeyAsToken
              case "ME" => ModeAdd
              case "MA" => ModeAppend
              case "MP" => ModePrepend
              case "MR" => ModeReplace
              case "MS" => ModeSet
              case _    => throw new IllegalArgumentException(s"Unknown flag $flag")
            }
        }
      new MetaSetFlags(flags.toSeq)
    }
}

class MetaSetFlags(override val flags: Seq[MetaSetFlag]) extends MetaFlagsBase[MetaSetFlag] {
  import MetaSetFlags._

  def casUnique: Option[CasUnique] = flags.collectFirst { case CompareCasToken(cas) => cas }

  def clientFlags: Option[Int] = flags.collectFirst { case SetClientFlags(value) => value }

  def expiration: Option[Long] = flags.collectFirst { case UpdateRemainingTTL(seconds) => seconds }

  def mode: MetaSetModeFlag = flags.collectFirst {
    case ModeAdd     => ModeAdd
    case ModeAppend  => ModeAppend
    case ModePrepend => ModePrepend
    case ModeReplace => ModeReplace
    case ModeSet     => ModeSet
  }.getOrElse(ModeSet)
}
