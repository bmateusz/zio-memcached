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

import zio.memcached.model.MetaDebugFlags.MetaDebugFlag

object MetaDebugFlags {

  /**
   * The flags used by the 'me' command
   */
  sealed trait MetaDebugFlag extends MetaFlagBase

  /**
   * interpret key as base64 encoded binary value
   *
   * This flag instructs memcached to run a base64 decoder on <key> before looking it up. This allows storing and
   * fetching of binary packed keys, so long as they are sent to memcached in base64 encoding.
   *
   * If 'b' flag is sent in the response, and a key is returned via 'k', this signals to the client that the key is
   * base64 encoded binary.
   */
  case object InterpretKeyAsBase64 extends MetaDebugFlag {
    override def flag: String = "b"
  }

  def apply(flags: Seq[MetaDebugFlag]): MetaDebugFlags = new MetaDebugFlags(flags)

  val empty = new MetaDebugFlags(Seq.empty)

  /**
   * Create a new [[MetaDebugFlags]] from a string of flags
   *
   * @param string
   *   the string of flags
   * @return
   *   a [[MetaDebugFlags]] object
   */
  def fromString(string: String): MetaDebugFlags =
    if (string.isEmpty) {
      empty
    } else {
      val flags = string
        .split(" ")
        .map {
          case "b"  => InterpretKeyAsBase64
          case flag => throw new IllegalArgumentException(s"Unknown flag $flag")
        }
      new MetaDebugFlags(flags.toSeq)
    }
}

class MetaDebugFlags(override val flags: Seq[MetaDebugFlag]) extends MetaFlagsBase[MetaDebugFlag]
