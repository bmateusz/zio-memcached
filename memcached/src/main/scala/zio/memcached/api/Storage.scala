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
import zio.memcached._
import zio.memcached.model.CasUnique
import zio.memcached.model.UpdateResult.UpdateResult
import zio.schema.Schema
import zio.schema.codec.Codec

trait Storage {

  /**
   * Get the value of a key.
   *
   * @param key
   *   Key to get the value of
   * @return
   *   Returns the value of the string or None if it does not exist.
   */
  final def get[R: Schema](key: String): ZIO[Memcached, MemcachedError, Option[R]] =
    MemcachedCommand(key, GetCommand(key), SingleGetOutput[R]())

  final def getByteChunk(key: String): ZIO[Memcached, MemcachedError, Option[Chunk[Byte]]] =
    MemcachedCommand(key, GetCommand(key), SingleChunkGetOutput)

  final def getWithCas[R: Schema](key: String): ZIO[Memcached, MemcachedError, Option[(CasUnique, R)]] =
    MemcachedCommand(key, GetsCommand(key), SingleGetWithCasOutput[R]())

  final def touch(key: String, expireTime: Duration): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(key, TouchCommand(key, expireTime), TouchOutput)

  final def getAndTouch[R: Schema](key: String, expireTime: Duration): ZIO[Memcached, MemcachedError, Option[R]] =
    MemcachedCommand(key, GatCommand(key, expireTime), SingleGetOutput[R]())

  final def getAndTouchWithCas[R: Schema](
    key: String,
    expireTime: Duration
  ): ZIO[Memcached, MemcachedError, Option[(CasUnique, R)]] =
    MemcachedCommand(key, GatsCommand(key, expireTime), SingleGetWithCasOutput[R]())

  final def set[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: Codec = memcached.codec
      MemcachedCommand(key, SetCommand[V](key, expireTime, value), SetOutput)
    }

  final def add[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: Codec = memcached.codec
      MemcachedCommand(key, AddCommand[V](key, expireTime, value), SetOutput)
    }

  final def replace[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: Codec = memcached.codec
      MemcachedCommand(key, ReplaceCommand[V](key, expireTime, value), SetOutput)
    }

  final def append[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: Codec = memcached.codec
      MemcachedCommand(key, AppendCommand[V](key, expireTime, value), SetOutput)
    }
  final def prepend[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: Codec = memcached.codec
      MemcachedCommand(key, PrependCommand[V](key, expireTime, value), SetOutput)
    }
  final def compareAndSet[V: Schema](
    key: String,
    value: V,
    casUnique: CasUnique,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, UpdateResult] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: Codec = memcached.codec
      MemcachedCommand(key, CompareAndSetCommand[V](key, casUnique, expireTime, value), UpdateResultOutput)
    }

  final def increment(key: String, value: Long): ZIO[Memcached, MemcachedError, Long] =
    if (value >= 0)
      MemcachedCommand(key, IncrementCommand(key, value), NumericOutput)
    else
      MemcachedCommand(key, DecrementCommand(key, -value), NumericOutput)

  final def delete(key: String): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(key, DeleteCommand(key), DeleteOutput)
}
