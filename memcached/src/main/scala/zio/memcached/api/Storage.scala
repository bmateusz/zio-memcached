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
    MemcachedCommand(new GetCommand(key), SingleGetOutput[R]()).run(())

  final def getWithCas[R: Schema](key: String): ZIO[Memcached, MemcachedError, Option[(CasUnique, R)]] =
    MemcachedCommand(new GetsCommand(key), SingleGetWithCasOutput[R]()).run(())

  final def touch(key: String, expireTime: Duration): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(new TouchCommand(key), TouchOutput).run(expireTime)

  final def getAndTouch[R: Schema](key: String, expireTime: Duration): ZIO[Memcached, MemcachedError, Option[R]] =
    MemcachedCommand(new GatCommand(key), SingleGetOutput[R]()).run(expireTime)

  final def getAndTouchWithCas[R: Schema](
    key: String,
    expireTime: Duration
  ): ZIO[Memcached, MemcachedError, Option[(CasUnique, R)]] =
    MemcachedCommand(new GatsCommand(key), SingleGetWithCasOutput[R]()).run(expireTime)

  final def set[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(new SetCommand[V](key), SetOutput).run((value, expireTime))

  final def add[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(new AddCommand[V](key), SetOutput).run((value, expireTime))

  final def replace[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(new ReplaceCommand[V](key), SetOutput).run((value, expireTime))

  final def append[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(new AppendCommand[V](key), SetOutput).run((value, expireTime))

  final def prepend[V: Schema](
    key: String,
    value: V,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(new PrependCommand[V](key), SetOutput).run((value, expireTime))

  final def compareAndSet[V: Schema](
    key: String,
    value: V,
    casUnique: CasUnique,
    expireTime: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, UpdateResult] =
    MemcachedCommand(new CompareAndSetCommand[V](key), UpdateResultOutput).run((value, casUnique, expireTime))

  final def increase(key: String, value: Long): ZIO[Memcached, MemcachedError, Long] =
    if (value >= 0)
      MemcachedCommand(new IncreaseCommand(key), NumericOutput).run(value)
    else
      MemcachedCommand(new DecreaseCommand(key), NumericOutput).run(-value)

  final def delete(key: String): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(new DeleteCommand(key), DeleteOutput).run(())
}
