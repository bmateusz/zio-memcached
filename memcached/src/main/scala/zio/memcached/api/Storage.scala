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
import zio.memcached.model.UpdateResult.UpdateResult
import zio.memcached.model.{CasUnique, ValueWithCasUnique}
import zio.schema.Schema
import zio.schema.codec.BinaryCodec

/**
 * Storage commands. See [[https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt]] for more
 * details.
 */
trait Storage {

  /**
   * Get the value of a key, decoding it as an `R`.
   *
   * @param key
   *   Key to get the value of
   * @tparam R
   *   Type of the value
   * @return
   *   Returns the value decoded or None if it does not exist.
   */
  final def get[R: Schema](key: String): ZIO[Memcached, MemcachedError, Option[R]] =
    MemcachedCommand(key, GetCommand(key), SingleGetOutput[R]())

  /**
   * Get the value of a key, decoding it as an `R`.
   *
   * @param key
   *   Key to get the value of
   * @tparam R
   *   Type of the value
   * @return
   *   Returns the value decoded and the unique "compare and swap token" or None if it does not exist.
   */
  final def getWithCas[R: Schema](key: String): ZIO[Memcached, MemcachedError, Option[ValueWithCasUnique[R]]] =
    MemcachedCommand(key, GetsCommand(key), SingleGetWithCasOutput[R]())

  /**
   * Touch the value of a key, updating the expiration time.
   *
   * @param key
   *   Key to touch
   * @param expiration
   *   Expiration time in seconds
   * @return
   *   Returns true if the key was touched, false if it does not exist.
   */
  final def touch(key: String, expiration: Duration): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(key, TouchCommand(key, expiration), TouchOutput)

  /**
   * Get and touch the value of a key, decoding it as an `R`.
   *
   * @param key
   *   Key to get and touch
   * @param expiration
   *   Expiration time in seconds
   * @tparam R
   *   Type of the value
   * @return
   *   Returns the value decoded or None if it does not exist.
   */
  final def getAndTouch[R: Schema](key: String, expiration: Duration): ZIO[Memcached, MemcachedError, Option[R]] =
    MemcachedCommand(key, GatCommand(key, expiration), SingleGetOutput[R]())

  /**
   * Get and touch the value of a key, decoding it as an `R` and updating the expiration time.
   *
   * @param key
   *   Key to get and touch
   * @param expiration
   *   Expiration time in seconds
   * @tparam R
   *   Type of the value
   * @return
   *   Returns the value decoded and the unique "compare and swap token" or None if it does not exist.
   */
  final def getAndTouchWithCas[R: Schema](
    key: String,
    expiration: Duration
  ): ZIO[Memcached, MemcachedError, Option[ValueWithCasUnique[R]]] =
    MemcachedCommand(key, GatsCommand(key, expiration), SingleGetWithCasOutput[R]())

  /**
   * Set the value of a key, encoding it as a `V`.
   *
   * @param key
   *   Key to set the value of
   * @param value
   *   Value to set
   * @param expiration
   *   Optional expiration time in seconds. If not provided, the value will never expire.
   * @tparam V
   *   Type of the value
   * @return
   *   Returns true if the value was set, false if it was not.
   */
  final def set[V: Schema](
    key: String,
    value: V,
    expiration: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: BinaryCodec = memcached.codec
      MemcachedCommand(key, SetCommand[V](key, expiration, value), SetOutput)
    }

  /**
   * Add the value of a key, encoding it as a `V` if it does not exist.
   *
   * @param key
   *   Key to add the value of
   * @param value
   *   Value to add
   * @param expiration
   *   Optional expiration time in seconds. If not provided, the value will never expire.
   * @tparam V
   *   Type of the value
   * @return
   *   Returns true if the value was added, false if it was not.
   */
  final def add[V: Schema](
    key: String,
    value: V,
    expiration: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: BinaryCodec = memcached.codec
      MemcachedCommand(key, AddCommand[V](key, expiration, value), SetOutput)
    }

  /**
   * Replace the value of a key, encoding it as a `V`, if it exists.
   *
   * @param key
   *   Key to replace the value of
   * @param value
   *   Value to replace
   * @param expiration
   *   Optional expiration time in seconds. If not provided, the value will never expire.
   * @tparam V
   *   Type of the value
   * @return
   *   Returns true if the value was replaced, false if it was not.
   */
  final def replace[V: Schema](
    key: String,
    value: V,
    expiration: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: BinaryCodec = memcached.codec
      MemcachedCommand(key, ReplaceCommand[V](key, expiration, value), SetOutput)
    }

  /**
   * Append the value of a key, encoding it as a `V`. It does a simple string append.
   *
   * @param key
   *   Key to change the value of
   * @param value
   *   Value to change
   * @param expiration
   *   Optional expiration time in seconds. If not provided, the value will never expire.
   * @tparam V
   *   Type of the value
   * @return
   *   Returns true if the value was changed, false if it was not.
   */
  final def append[V: Schema](
    key: String,
    value: V,
    expiration: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: BinaryCodec = memcached.codec
      MemcachedCommand(key, AppendCommand[V](key, expiration, value), SetOutput)
    }

  /**
   * Prepend the value of a key, encoding it as a `V`. It does a simple string prepend.
   *
   * @param key
   *   Key to change the value of
   * @param value
   *   Value to change
   * @param expiration
   *   Optional expiration time in seconds. If not provided, the value will never expire.
   * @tparam V
   *   Type of the value
   * @return
   *   Returns true if the value was changed, false if it was not.
   */
  final def prepend[V: Schema](
    key: String,
    value: V,
    expiration: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, Boolean] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: BinaryCodec = memcached.codec
      MemcachedCommand(key, PrependCommand[V](key, expiration, value), SetOutput)
    }

  /**
   * Conditional set the value of a key, encoding it as a `V`, if the unique "compare and swap token" matches.
   *
   * You can get the unique "compare and swap token" by using the `getWithCas` method.
   *
   * @param key
   *   Key to change the value of
   * @param value
   *   Value to change
   * @param expiration
   *   Optional expiration time in seconds. If not provided, the value will never expire.
   * @tparam V
   *   Type of the value
   * @return
   *   Returns `Updated` if the value was changed, `Exists` if the value was found, but the "compare and swap token" did
   *   not match, and `NotFound` if the value was not found.
   */
  final def compareAndSwap[V: Schema](
    key: String,
    value: V,
    casUnique: CasUnique,
    expiration: Option[Duration] = None
  ): ZIO[Memcached, MemcachedError, UpdateResult] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: BinaryCodec = memcached.codec
      MemcachedCommand(key, CompareAndSwapCommand[V](key, casUnique, expiration, value), UpdateResultOutput)
    }

  /**
   * Increment the value of a key by a given amount.
   *
   * Be careful with it, memcached arithmetic operations only work on strings that represent numbers.
   *
   * @param key
   *   Key to increment the value of
   * @param value
   *   Amount to increment by. Can be negative.
   * @return
   *   Returns the new value of the key, or `None` if the key was not found or the value was not a number.
   */
  final def increment(key: String, value: Long): ZIO[Memcached, MemcachedError, Option[Long]] =
    if (value >= 0)
      MemcachedCommand(key, IncrementCommand(key, value), NumericOutput)
    else
      MemcachedCommand(key, DecrementCommand(key, -value), NumericOutput)

  /**
   * Delete a key.
   *
   * @param key
   *   Key to delete
   * @return
   *   Returns true if the key was deleted, false if it was not found.
   */
  final def delete(key: String): ZIO[Memcached, MemcachedError, Boolean] =
    MemcachedCommand(key, DeleteCommand(key), DeleteOutput)
}
