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
import zio.memcached.model.MetaGetFlags.MetaGetFlag

object MetaGetFlags {

  /**
   * The flags used by the 'mg' command
   */
  sealed trait MetaGetFlag extends MetaFlagBase

  /**
   * interpret key as base64 encoded binary value
   *
   * This flag instructs memcached to run a base64 decoder on <key> before looking it up. This allows storing and
   * fetching of binary packed keys, so long as they are sent to memcached in base64 encoding.
   *
   * If 'b' flag is sent in the response, and a key is returned via 'k', this signals to the client that the key is
   * base64 encoded binary.
   */
  case object InterpretKeyAsBase64 extends MetaGetFlag {
    override def flag: String = "b"
  }

  /**
   * return item cas token
   */
  case object ReturnItemCasUnique extends MetaGetFlag {
    override def flag: String = "c"
  }

  /**
   * return client flags token
   */
  case object ReturnClientFlags extends MetaGetFlag {
    override def flag: String = "f"
  }

  /**
   * return whether item has been hit before as a 0 or 1
   */
  case object ReturnWhetherItemHasBeenHitBefore extends MetaGetFlag {
    override def flag: String = "h"
  }

  /**
   * return key as a token
   */
  case object ReturnKeyAsToken extends MetaGetFlag {
    override def flag: String = "k"
  }

  /**
   * return time since item was last accessed in seconds
   */
  case object ReturnTimeSinceItemWasLastAccessed extends MetaGetFlag {
    override def flag: String = "l"
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
  case class Opaque private (token: String) extends MetaGetFlag {
    override def flag: String = s"O$token"
  }

  object Opaque {
    def apply(token: String): Opaque =
      if (isValidKey(token)) new Opaque(token) else throw new IllegalArgumentException("Invalid token for opaque flag")
  }

  /**
   * return item size token
   */
  case object ReturnItemSize extends MetaGetFlag {
    override def flag: String = "s"
  }

  /**
   * return item TTL remaining in seconds (-1 for unlimited)
   */
  case object ReturnItemTTL extends MetaGetFlag {
    override def flag: String = "t"
  }

  /**
   * don't bump the item in the LRU
   */
  case object DontBumpInLRU extends MetaGetFlag {
    override def flag: String = "u"
  }

  /**
   * return item value in <data block>
   */
  case object ReturnItemValue extends MetaGetFlag {
    override def flag: String = "v"
  }

  /**
   * These flags can modify the item
   */
  sealed trait MetaGetModifiedFlag extends MetaGetFlag

  /**
   * vivify on miss, takes TTL as a argument
   *
   * @param seconds
   *   new ttl
   */
  case class VivifyOnMiss(seconds: Long) extends MetaGetModifiedFlag {
    override def flag: String = s"N$seconds"
  }

  /**
   * if token is less than remaining TTL win for recache
   *
   * @param seconds
   *   ttl to compare
   */
  case class WinForRecacheIfTokenIsLessThan(seconds: Long) extends MetaGetModifiedFlag {
    override def flag: String = s"R$seconds"
  }

  /**
   * update remaining TTL
   *
   * @param seconds
   *   new ttl
   */
  case class UpdateRemainingTTL(seconds: Long) extends MetaGetModifiedFlag {
    override def flag: String = s"T$seconds"
  }

  def apply(flags: Seq[MetaGetFlag]): MetaGetFlags = new MetaGetFlags(flags)

  val empty = new MetaGetFlags(Seq.empty)

  private val FlagWithValueRegex = """([NRTO])(.+)""".r

  /**
   * Create a new [[MetaGetFlags]] from a string of flags
   *
   * @param string
   *   the string of flags
   * @return
   *   a [[MetaGetFlags]] object
   */
  def fromString(string: String): MetaGetFlags =
    if (string.isEmpty) {
      empty
    } else {
      val flags = string
        .split(" ")
        .map {
          case FlagWithValueRegex(flag, value) =>
            flag match {
              case "N" => VivifyOnMiss(value.toLong)
              case "R" => WinForRecacheIfTokenIsLessThan(value.toLong)
              case "T" => UpdateRemainingTTL(value.toLong)
              case "O" => Opaque(value)
            }
          case flag =>
            flag match {
              case "b" => InterpretKeyAsBase64
              case "c" => ReturnItemCasUnique
              case "f" => ReturnClientFlags
              case "h" => ReturnWhetherItemHasBeenHitBefore
              case "k" => ReturnKeyAsToken
              case "l" => ReturnTimeSinceItemWasLastAccessed
              case "s" => ReturnItemSize
              case "t" => ReturnItemTTL
              case "u" => DontBumpInLRU
              case "v" => ReturnItemValue
            }
        }
      new MetaGetFlags(flags.toSeq)
    }
}

class MetaGetFlags(override val flags: Seq[MetaGetFlag]) extends MetaFlagsBase[MetaGetFlag]
