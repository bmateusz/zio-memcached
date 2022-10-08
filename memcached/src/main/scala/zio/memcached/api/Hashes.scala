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
import zio.memcached.ResultBuilder._
import zio.memcached._
import zio.schema.Schema

trait Hashes {
  import Hashes._

  /**
   * Removes the specified fields from the hash stored at `key`.
   *
   * @param key
   *   of the hash that should be removed
   * @param field
   *   field to remove
   * @param fields
   *   additional fields
   * @return
   *   number of fields removed from the hash.
   */
  final def hDel[K: Schema, F: Schema](key: K, field: F, fields: F*): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(HDel, Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[F]())), LongOutput)
    command.run((key, (field, fields.toList)))
  }

  /**
   * Returns if `field` is an existing field in the hash stored at `key`.
   *
   * @param key
   *   of the hash that should be inspected
   * @param field
   *   field to inspect
   * @return
   *   true if the field exists, otherwise false.
   */
  final def hExists[K: Schema, F: Schema](key: K, field: F): ZIO[Memcached, MemcachedError, Boolean] = {
    val command = MemcachedCommand(HExists, Tuple2(ArbitraryInput[K](), ArbitraryInput[F]()), BoolOutput)
    command.run((key, field))
  }

  /**
   * Returns the value associated with `field` in the hash stored at `key`.
   *
   * @param key
   *   of the hash whose field should be read
   * @param field
   *   which value should be returned
   * @return
   *   value stored in the field, if any.
   */
  final def hGet[K: Schema, F: Schema](key: K, field: F): ResultBuilder1[Option] =
    new ResultBuilder1[Option] {
      def returning[V: Schema]: ZIO[Memcached, MemcachedError, Option[V]] =
        MemcachedCommand(HGet, Tuple2(ArbitraryInput[K](), ArbitraryInput[F]()), OptionalOutput(ArbitraryOutput[V]()))
          .run((key, field))
    }

  /**
   * Returns all fields and values of the hash stored at `key`.
   *
   * @param key
   *   of the hash that should be read
   * @return
   *   map of `field -> value` pairs under the key.
   */
  final def hGetAll[K: Schema](key: K): ResultBuilder2[Map] = new ResultBuilder2[Map] {
    def returning[F: Schema, V: Schema]: ZIO[Memcached, MemcachedError, Map[F, V]] = {
      val command =
        MemcachedCommand(HGetAll, ArbitraryInput[K](), KeyValueOutput(ArbitraryOutput[F](), ArbitraryOutput[V]()))
      command.run(key)
    }
  }

  /**
   * Increments the number stored at `field` in the hash stored at `key` by `increment`. If field does not exist the
   * value is set to `increment`.
   *
   * @param key
   *   of the hash that should be updated
   * @param field
   *   field containing an integer, or a new field
   * @param increment
   *   integer to increment with
   * @return
   *   integer value after incrementing, or error if the field is not an integer.
   */
  final def hIncrBy[K: Schema, F: Schema](key: K, field: F, increment: Long): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(HIncrBy, Tuple3(ArbitraryInput[K](), ArbitraryInput[F](), LongInput), LongOutput)
    command.run((key, field, increment))
  }

  /**
   * Increment the specified `field` of a hash stored at `key`, and representing a floating point number by the
   * specified `increment`.
   *
   * @param key
   *   of the hash that should be updated
   * @param field
   *   field containing a float, or a new field
   * @param increment
   *   float to increment with
   * @return
   *   float value after incrementing, or error if the field is not a float.
   */
  final def hIncrByFloat[K: Schema, F: Schema](
    key: K,
    field: F,
    increment: Double
  ): ZIO[Memcached, MemcachedError, Double] = {
    val command =
      MemcachedCommand(HIncrByFloat, Tuple3(ArbitraryInput[K](), ArbitraryInput[F](), DoubleInput), DoubleOutput)
    command.run((key, field, increment))
  }

  /**
   * Returns all field names in the hash stored at `key`.
   *
   * @param key
   *   of the hash whose fields should be returned
   * @return
   *   chunk of field names.
   */
  final def hKeys[K: Schema](key: K): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[F: Schema]: ZIO[Memcached, MemcachedError, Chunk[F]] =
        MemcachedCommand(HKeys, ArbitraryInput[K](), ChunkOutput(ArbitraryOutput[F]())).run(key)
    }

  /**
   * Returns the number of fields contained in the hash stored at `key`.
   *
   * @param key
   *   of the hash whose fields should be counted
   * @return
   *   number of fields.
   */
  final def hLen[K: Schema](key: K): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(HLen, ArbitraryInput[K](), LongOutput)
    command.run(key)
  }

  /**
   * Returns the values associated with the specified `fields` in the hash stored at `key`.
   *
   * @param key
   *   of the hash whose values should be read
   * @param field
   *   fields to retrieve
   * @param fields
   *   additional fields
   * @return
   *   chunk of values, where value is `None` if the field is not in the hash.
   */
  final def hmGet[K: Schema, F: Schema](
    key: K,
    field: F,
    fields: F*
  ): ResultBuilder1[({ type lambda[x] = Chunk[Option[x]] })#lambda] =
    new ResultBuilder1[({ type lambda[x] = Chunk[Option[x]] })#lambda] {
      def returning[V: Schema]: ZIO[Memcached, MemcachedError, Chunk[Option[V]]] = {
        val command = MemcachedCommand(
          HmGet,
          Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[F]())),
          ChunkOutput(OptionalOutput(ArbitraryOutput[V]()))
        )
        command.run((key, (field, fields.toList)))
      }
    }

  /**
   * Sets the specified `field -> value` pairs in the hash stored at `key`. Deprecated: As per Memcached 4.0.0, HMSET is
   * considered deprecated. Please use `hSet` instead.
   *
   * @param key
   *   of the hash whose value should be set
   * @param pair
   *   mapping of a field to value
   * @param pairs
   *   additional pairs
   * @return
   *   unit if fields are successfully set.
   */
  final def hmSet[K: Schema, F: Schema, V: Schema](
    key: K,
    pair: (F, V),
    pairs: (F, V)*
  ): ZIO[Memcached, MemcachedError, Unit] = {
    val command = MemcachedCommand(
      HmSet,
      Tuple2(ArbitraryInput[K](), NonEmptyList(Tuple2(ArbitraryInput[F](), ArbitraryInput[V]()))),
      UnitOutput
    )
    command.run((key, (pair, pairs.toList)))
  }

  /**
   * Iterates `fields` of Hash types and their associated values using a cursor-based iterator
   *
   * @param key
   *   of the hash that should be scanned
   * @param cursor
   *   integer representing iterator
   * @param pattern
   *   regular expression matching keys to scan
   * @param count
   *   approximate number of elements to return (see https://memcached.io/commands/scan#the-count-option)
   * @return
   *   pair containing the next cursor and list of elements scanned.
   */
  final def hScan[K: Schema](
    key: K,
    cursor: Long,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ResultBuilder2[({ type lambda[x, y] = (Long, Chunk[(x, y)]) })#lambda] =
    new ResultBuilder2[({ type lambda[x, y] = (Long, Chunk[(x, y)]) })#lambda] {
      def returning[F: Schema, V: Schema]: ZIO[Memcached, MemcachedError, (Long, Chunk[(F, V)])] = {
        val command = MemcachedCommand(
          HScan,
          Tuple4(ArbitraryInput[K](), LongInput, OptionalInput(PatternInput), OptionalInput(CountInput)),
          Tuple2Output(ArbitraryOutput[Long](), ChunkTuple2Output(ArbitraryOutput[F](), ArbitraryOutput[V]()))
        )
        command.run((key, cursor, pattern.map(Pattern(_)), count))
      }
    }

  /**
   * Sets `field -> value` pairs in the hash stored at `key`
   *
   * @param key
   *   of the hash whose value should be set
   * @param pair
   *   mapping of a field to value
   * @param pairs
   *   additional pairs
   * @return
   *   number of fields added.
   */
  final def hSet[K: Schema, F: Schema, V: Schema](
    key: K,
    pair: (F, V),
    pairs: (F, V)*
  ): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(
      HSet,
      Tuple2(ArbitraryInput[K](), NonEmptyList(Tuple2(ArbitraryInput[F](), ArbitraryInput[V]()))),
      LongOutput
    )
    command.run((key, (pair, pairs.toList)))
  }

  /**
   * Sets `field` in the hash stored at `key` to `value`, only if `field` does not yet exist
   *
   * @param key
   *   of the hash whose value should be set
   * @param field
   *   for which a value should be set
   * @param value
   *   that should be set
   * @return
   *   true if `field` is a new field and value was set, otherwise false.
   */
  final def hSetNx[K: Schema, F: Schema, V: Schema](
    key: K,
    field: F,
    value: V
  ): ZIO[Memcached, MemcachedError, Boolean] = {
    val command =
      MemcachedCommand(HSetNx, Tuple3(ArbitraryInput[K](), ArbitraryInput[F](), ArbitraryInput[V]()), BoolOutput)
    command.run((key, field, value))
  }

  /**
   * Returns the string length of the value associated with `field` in the hash stored at `key`
   *
   * @param key
   *   of the hash that should be read
   * @param field
   *   which value length should be returned
   * @return
   *   string length of the value in field, or zero if either field or key do not exist.
   */
  final def hStrLen[K: Schema, F: Schema](key: K, field: F): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(HStrLen, Tuple2(ArbitraryInput[K](), ArbitraryInput[F]()), LongOutput)
    command.run((key, field))
  }

  /**
   * Returns all values in the hash stored at `key`
   *
   * @param key
   *   of the hash which values should be read
   * @return
   *   list of values in the hash, or an empty list when `key` does not exist.
   */
  final def hVals[K: Schema](key: K): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[V: Schema]: ZIO[Memcached, MemcachedError, Chunk[V]] =
        MemcachedCommand(HVals, ArbitraryInput[K](), ChunkOutput(ArbitraryOutput[V]())).run(key)
    }

  /**
   * Returns a random field from the hash value stored at `key`
   *
   * @param key
   *   of the hash which fields should be read
   * @return
   *   random field in the hash or `None` when `key` does not exist.
   */
  final def hRandField[K: Schema](key: K): ResultBuilder1[Option] =
    new ResultBuilder1[Option] {
      def returning[V: Schema]: ZIO[Memcached, MemcachedError, Option[V]] =
        MemcachedCommand(HRandField, ArbitraryInput[K](), OptionalOutput(ArbitraryOutput[V]())).run(key)
    }

  /**
   * Returns an array of at most `count` distinct fields randomly. If called with a negative `count`, the command will
   * return exactly `count` fields, allowing repeats. If `withValues` is true it will return fields with his values
   * intercalated.
   *
   * @param key
   *   of the hash which fields should be read
   * @param count
   *   maximum number of different fields to return if positive or the exact number, in absolute value if negative
   * @param withValues
   *   when true includes the respective values of the randomly selected hash fields
   * @return
   *   a list of fields or fields and values if `withValues` is true.
   */
  final def hRandField[K: Schema](key: K, count: Long, withValues: Boolean = false): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[V: Schema]: ZIO[Memcached, MemcachedError, Chunk[V]] = {
        val command = MemcachedCommand(
          HRandField,
          Tuple3(ArbitraryInput[K](), LongInput, OptionalInput(StringInput)),
          ChunkOutput(ArbitraryOutput[V]())
        )
        command.run((key, count, if (withValues) Some("WITHVALUES") else None))
      }
    }
}

private[memcached] object Hashes {
  final val HDel         = "HDEL"
  final val HExists      = "HEXISTS"
  final val HGet         = "HGET"
  final val HGetAll      = "HGETALL"
  final val HIncrBy      = "HINCRBY"
  final val HIncrByFloat = "HINCRBYFLOAT"
  final val HKeys        = "HKEYS"
  final val HLen         = "HLEN"
  final val HmGet        = "HMGET"
  final val HmSet        = "HMSET"
  final val HScan        = "HSCAN"
  final val HSet         = "HSET"
  final val HSetNx       = "HSETNX"
  final val HStrLen      = "HSTRLEN"
  final val HVals        = "HVALS"
  final val HRandField   = "HRANDFIELD"
}
