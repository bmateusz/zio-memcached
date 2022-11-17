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

import zio.ZIO
import zio.memcached.Input._
import zio.memcached.Output._
import zio.memcached.model.MetaArithmeticFlags.MetaArithmeticFlag
import zio.memcached.model.MetaDebugFlags.MetaDebugFlag
import zio.memcached.model.MetaDeleteFlags.MetaDeleteFlag
import zio.memcached.model.MetaGetFlags.MetaGetFlag
import zio.memcached.model.MetaResult.{
  MetaArithmeticResult,
  MetaDebugResult,
  MetaDeleteResult,
  MetaGetResult,
  MetaSetResult
}
import zio.memcached.model.MetaSetFlags.MetaSetFlag
import zio.memcached.model.{MetaArithmeticFlags, MetaDebugFlags, MetaDeleteFlags, MetaGetFlags, MetaSetFlags}
import zio.memcached.{Memcached, MemcachedCommand, MemcachedError}
import zio.schema.Schema
import zio.schema.codec.Codec

/**
 * Meta commands. See [[https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt]] for more
 * details.
 */
trait Meta {

  /**
   * Get the value of a key, decoding it as an `R`.
   *
   * @param key
   *   Key to get the value of
   * @param flags
   *   Meta get flags provided as varargs
   * @tparam R
   *   Type of the value
   * @return
   *   Returns the value decoded and the headers or None if it does not exist. See
   *   [[zio.memcached.model.MetaResult.MetaGetResult]].
   */
  def metaGet[R: Schema](key: String, flags: MetaGetFlag*): ZIO[Memcached, MemcachedError, MetaGetResult[R]] =
    metaGet(key, MetaGetFlags(flags))

  /**
   * Get the value of a key, decoding it as an `R`.
   *
   * @param key
   *   Key to get the value of
   * @param flags
   *   Meta get flags provided in a single class
   * @tparam R
   *   Type of the value
   * @return
   *   Returns the value decoded and the headers or None if it does not exist. See
   *   [[zio.memcached.model.MetaResult.MetaGetResult]].
   */
  def metaGet[R: Schema](key: String, flags: MetaGetFlags): ZIO[Memcached, MemcachedError, MetaGetResult[R]] =
    MemcachedCommand(key, MetaGetCommand(key, flags), MetaGetOutput[R]())

  /**
   * Set the value of a key, encoding it as an `R`.
   *
   * @param key
   *   Key to set the value of
   * @param value
   *   Value to set
   * @param flags
   *   Meta set flags provided as varargs
   * @tparam R
   *   Type of the value
   * @return
   *   Depending on the headers given in the request, the result will be either "stored", "not stored", "exists" or "not
   *   found". See [[zio.memcached.model.MetaResult.MetaSetResult]].
   */
  def metaSet[R: Schema](key: String, value: R, flags: MetaSetFlag*): ZIO[Memcached, MemcachedError, MetaSetResult] =
    metaSet[R](key, value, MetaSetFlags(flags))

  /**
   * Set the value of a key, encoding it as an `R`.
   *
   * @param key
   *   Key to set the value of
   * @param value
   *   Value to set
   * @param flags
   *   Meta set flags provided in a single class
   * @tparam R
   *   Type of the value
   * @return
   *   Depending on the headers given in the request, the result will be either "stored", "not stored", "exists" or "not
   *   found". See [[zio.memcached.model.MetaResult.MetaSetResult]].
   */
  def metaSet[R: Schema](key: String, value: R, flags: MetaSetFlags): ZIO[Memcached, MemcachedError, MetaSetResult] =
    ZIO.serviceWithZIO[Memcached] { memcached =>
      implicit val codec: Codec = memcached.codec
      MemcachedCommand(key, MetaSetCommand[R](key, value, flags), MetaSetOutput)
    }

  /**
   * Delete a key.
   *
   * @param key
   *   Key to delete
   * @param flags
   *   Meta delete flags provided as varargs
   * @return
   *   Depending on the headers given in the request, the result will be either "deleted", "not found" or "exists". See
   *   [[zio.memcached.model.MetaResult.MetaDeleteResult]].
   */
  def metaDelete(key: String, flags: MetaDeleteFlag*): ZIO[Memcached, MemcachedError, MetaDeleteResult] =
    metaDelete(key, MetaDeleteFlags(flags))

  /**
   * Delete a key.
   *
   * @param key
   *   Key to delete
   * @param flags
   *   Meta delete flags provided in a single class
   * @return
   *   Depending on the headers given in the request, the result will be either "deleted", "not found" or "exists". See
   *   [[zio.memcached.model.MetaResult.MetaDeleteResult]].
   */
  def metaDelete(key: String, flags: MetaDeleteFlags): ZIO[Memcached, MemcachedError, MetaDeleteResult] =
    MemcachedCommand(key, MetaDeleteCommand(key, flags), MetaDeleteOutput)

  /**
   * Perform an arithmetic operation on a key.
   *
   * Be careful with it, memcached arithmetic operations only work on strings that represent numbers.
   *
   * @param key
   *   Key to perform the operation on
   * @param flags
   *   Meta arithmetic flags provided as varargs
   * @return
   *   Depending on the headers given in the request, the result will be either the new value or an error that the key
   *   "exists", "not found" or "not stored". See [[zio.memcached.model.MetaResult.MetaArithmeticResult]].
   */
  def metaArithmetic(key: String, flags: MetaArithmeticFlag*): ZIO[Memcached, MemcachedError, MetaArithmeticResult] =
    metaArithmetic(key, MetaArithmeticFlags(flags))

  /**
   * Perform an arithmetic operation on a key.
   *
   * Be careful with it, memcached arithmetic operations only work on strings that represent numbers.
   *
   * @param key
   *   Key to perform the operation on
   * @param flags
   *   Meta arithmetic flags provided in a single class
   * @return
   *   Depending on the headers given in the request, the result will be either the new value or an error that the key
   *   "exists", "not found" or "not stored". See [[zio.memcached.model.MetaResult.MetaArithmeticResult]].
   */
  def metaArithmetic(key: String, flags: MetaArithmeticFlags): ZIO[Memcached, MemcachedError, MetaArithmeticResult] =
    MemcachedCommand(key, MetaArithmeticCommand(key, flags), MetaArithmeticOutput)

  /**
   * Perform a debug operation on a key.
   *
   * @param key
   *   Key to perform the operation on
   * @param flags
   *   Meta debug flags provided as varargs
   * @return
   *   The result of the debug operation. See [[zio.memcached.model.MetaResult.MetaDebugResult]].
   */
  def metaDebug(key: String, flags: MetaDebugFlag*): ZIO[Memcached, MemcachedError, MetaDebugResult] =
    metaDebug(key, MetaDebugFlags(flags))

  /**
   * Perform a debug operation on a key.
   *
   * @param key
   *   Key to perform the operation on
   * @param flags
   *   Meta debug flags provided in a single class
   * @return
   *   The result of the debug operation. See [[zio.memcached.model.MetaResult.MetaDebugResult]].
   */
  def metaDebug(key: String, flags: MetaDebugFlags): ZIO[Memcached, MemcachedError, MetaDebugResult] =
    MemcachedCommand(key, MetaDebugCommand(key, flags), MetaDebugOutput)
}
