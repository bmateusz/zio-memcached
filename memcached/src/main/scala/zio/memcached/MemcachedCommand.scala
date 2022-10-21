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

package zio.memcached

import zio._

final class MemcachedCommand[-In, +Out] private (val input: Input[In], val output: Output[Out]) {
  private[memcached] def run(in: In): ZIO[Memcached, MemcachedError, Out] =
    ZIO
      .serviceWithZIO[Memcached] { memcached =>
        val command = input.encode(in)(memcached.codec)

        memcached.executor
          .execute(input.keyChunk.length, command) // TODO: use a better hash code
          .flatMap[Any, Throwable, Out](out => ZIO.attempt(output.unsafeDecode(out)(memcached.codec)))
      }
      .refineToOrDie[MemcachedError]
}

object MemcachedCommand {
  private[memcached] def apply[In, Out](input: Input[In], output: Output[Out]): MemcachedCommand[In, Out] =
    new MemcachedCommand(input, output)

  def validateKey(key: String): Boolean =
    key.length <= 250 && !key.exists((c: Char) => c.isControl || c.isWhitespace)
}
