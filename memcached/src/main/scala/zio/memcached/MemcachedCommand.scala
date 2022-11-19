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

object MemcachedCommand {
  private[memcached] def apply[Out](
    key: String,
    input: Chunk[Byte],
    output: Output[Out]
  ): ZIO[Memcached, MemcachedError, Out] =
    ZIO
      .serviceWithZIO[Memcached] { memcached =>
        if (isValidKey(key)) {
          memcached.executor
            .execute(key, input)
            .flatMap[Any, Throwable, Out](out => ZIO.attempt(output.unsafeDecode(out)(memcached.codec)))
        } else {
          ZIO.fail(MemcachedError.InvalidKey)
        }
      }
      .refineToOrDie[MemcachedError]

  def isValidKey(key: String): Boolean =
    key.nonEmpty && key.lengthCompare(250) <= 0 && !key.exists((c: Char) => c.isControl || c.isWhitespace)
}
