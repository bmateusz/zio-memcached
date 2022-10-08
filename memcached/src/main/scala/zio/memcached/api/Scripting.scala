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

package zio.memcached.api

import zio._
import zio.memcached.Input._
import zio.memcached.Output._
import zio.memcached.ResultBuilder.ResultOutputBuilder
import zio.memcached._

trait Scripting {
  import Scripting._

  /**
   * Evaluates a Lua script.
   *
   * @param script
   *   Lua script
   * @param keys
   *   keys available through KEYS param in the script
   * @param args
   *   values available through ARGV param in the script
   * @return
   *   memcached protocol value that is converted from the Lua type. You have to write decoder that would convert memcached
   *   protocol value to a suitable type for your app
   */
  def eval[K: Input, A: Input](
    script: String,
    keys: Chunk[K],
    args: Chunk[A]
  ): ResultOutputBuilder = new ResultOutputBuilder {
    def returning[R: Output]: ZIO[Memcached, MemcachedError, R] = {
      val command = MemcachedCommand(Eval, EvalInput(Input[K], Input[A]), Output[R])
      command.run((script, keys, args))
    }
  }

  /**
   * Evaluates a Lua script cached on the server side by its SHA1 digest. Scripts could be cached using the
   * [[zio.memcached.api.Scripting.scriptLoad]] method.
   *
   * @param sha1
   *   SHA1 digest
   * @param keys
   *   keys available through KEYS param in the script
   * @param args
   *   values available through ARGV param in the script
   * @return
   *   memcached protocol value that is converted from the Lua type. You have to write decoder that would convert memcached
   *   protocol value to a suitable type for your app
   */
  def evalSha[K: Input, A: Input](
    sha1: String,
    keys: Chunk[K],
    args: Chunk[A]
  ): ResultOutputBuilder = new ResultOutputBuilder {
    def returning[R: Output]: ZIO[Memcached, MemcachedError, R] = {
      val command = MemcachedCommand(EvalSha, EvalInput(Input[K], Input[A]), Output[R])
      command.run((sha1, keys, args))
    }
  }

  /**
   * Checks existence of the scripts in the script cache.
   *
   * @param sha1
   *   one required SHA1 digest
   * @param sha1s
   *   maybe rest of the SHA1 digests
   * @return
   *   for every corresponding SHA1 digest of a script that actually exists in the script cache, an true is returned,
   *   otherwise false is returned.
   */
  def scriptExists(sha1: String, sha1s: String*): ZIO[Memcached, MemcachedError, Chunk[Boolean]] = {
    val command = MemcachedCommand(ScriptExists, NonEmptyList(StringInput), ChunkOutput(BoolOutput))
    command.run((sha1, sha1s.toList))
  }

  /**
   * Loads a script into the scripts cache. After the script is loaded into the script cache it could be evaluated using
   * the [[zio.memcached.api.Scripting.evalSha]] method.
   *
   * @param script
   *   Lua script
   * @return
   *   the SHA1 digest of the script added into the script cache.
   */
  def scriptLoad(script: String): ZIO[Memcached, MemcachedError, String] = {
    val command = MemcachedCommand(ScriptLoad, StringInput, MultiStringOutput)
    command.run(script)
  }
}

private[memcached] object Scripting {
  final val Eval         = "EVAL"
  final val EvalSha      = "EVALSHA"
  final val ScriptExists = "SCRIPT EXISTS"
  final val ScriptLoad   = "SCRIPT LOAD"
}
