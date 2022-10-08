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

trait Sets {
  import Sets._

  /**
   * Add one or more members to a set.
   *
   * @param key
   *   Key of set to add to
   * @param member
   *   first member to add
   * @param members
   *   subsequent members to add
   * @return
   *   Returns the number of elements that were added to the set, not including all the elements already present into
   *   the set.
   */
  final def sAdd[K: Schema, M: Schema](key: K, member: M, members: M*): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(SAdd, Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[M]())), LongOutput)
    command.run((key, (member, members.toList)))
  }

  /**
   * Get the number of members in a set.
   *
   * @param key
   *   Key of set to get the number of members of
   * @return
   *   Returns the cardinality (number of elements) of the set, or 0 if key does not exist.
   */
  final def sCard[K: Schema](key: K): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(SCard, ArbitraryInput[K](), LongOutput)
    command.run(key)
  }

  /**
   * Subtract multiple sets.
   *
   * @param key
   *   Key of the set to subtract from
   * @param keys
   *   Keys of the sets to subtract
   * @return
   *   Returns the members of the set resulting from the difference between the first set and all the successive sets.
   */
  final def sDiff[K: Schema](key: K, keys: K*): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[R: Schema]: ZIO[Memcached, MemcachedError, Chunk[R]] =
        MemcachedCommand(SDiff, NonEmptyList(ArbitraryInput[K]()), ChunkOutput(ArbitraryOutput[R]()))
          .run((key, keys.toList))
    }

  /**
   * Subtract multiple sets and store the resulting set in a key.
   *
   * @param destination
   *   Key of set to store the resulting set
   * @param key
   *   Key of set to be subtracted from
   * @param keys
   *   Keys of sets to subtract
   * @return
   *   Returns the number of elements in the resulting set.
   */
  final def sDiffStore[D: Schema, K: Schema](destination: D, key: K, keys: K*): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(SDiffStore, Tuple2(ArbitraryInput[D](), NonEmptyList(ArbitraryInput[K]())), LongOutput)
    command.run((destination, (key, keys.toList)))
  }

  /**
   * Intersect multiple sets and store the resulting set in a key.
   *
   * @param destination
   *   Key of set to store the resulting set
   * @param keys
   *   Keys of the sets to intersect with each other
   * @return
   *   Returns the members of the set resulting from the intersection of all the given sets.
   */
  final def sInter[K: Schema](destination: K, keys: K*): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[R: Schema]: ZIO[Memcached, MemcachedError, Chunk[R]] =
        MemcachedCommand(SInter, NonEmptyList(ArbitraryInput[K]()), ChunkOutput(ArbitraryOutput[R]()))
          .run((destination, keys.toList))
    }

  /**
   * Intersect multiple sets and store the resulting set in a key.
   *
   * @param destination
   *   Key of set to store the resulting set
   * @param key
   *   Key of first set to intersect
   * @param keys
   *   Keys of subsequent sets to intersect
   * @return
   *   Returns the number of elements in the resulting set.
   */
  final def sInterStore[D: Schema, K: Schema](
    destination: D,
    key: K,
    keys: K*
  ): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(SInterStore, Tuple2(ArbitraryInput[D](), NonEmptyList(ArbitraryInput[K]())), LongOutput)
    command.run((destination, (key, keys.toList)))
  }

  /**
   * Determine if a given value is a member of a set.
   *
   * @param key
   *   of the set
   * @param member
   *   value which should be searched in the set
   * @return
   *   Returns 1 if the element is a member of the set. 0 if the element is not a member of the set, or if key does not
   *   exist.
   */
  final def sIsMember[K: Schema, M: Schema](key: K, member: M): ZIO[Memcached, MemcachedError, Boolean] = {
    val command = MemcachedCommand(SIsMember, Tuple2(ArbitraryInput[K](), ArbitraryInput[M]()), BoolOutput)
    command.run((key, member))
  }

  /**
   * Get all the members in a set.
   *
   * @param key
   *   Key of the set to get the members of
   * @return
   *   Returns the members of the set.
   */
  final def sMembers[K: Schema](key: K): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[R: Schema]: ZIO[Memcached, MemcachedError, Chunk[R]] =
        MemcachedCommand(SMembers, ArbitraryInput[K](), ChunkOutput(ArbitraryOutput[R]())).run(key)
    }

  /**
   * Move a member from one set to another.
   *
   * @param source
   *   Key of the set to move the member from
   * @param destination
   *   Key of the set to move the member to
   * @param member
   *   Element to move
   * @return
   *   Returns 1 if the element was moved. 0 if it was not found.
   */
  final def sMove[S: Schema, D: Schema, M: Schema](
    source: S,
    destination: D,
    member: M
  ): ZIO[Memcached, MemcachedError, Boolean] = {
    val command = MemcachedCommand(SMove, Tuple3(ArbitraryInput[S](), ArbitraryInput[D](), ArbitraryInput[M]()), BoolOutput)
    command.run((source, destination, member))
  }

  /**
   * Remove and return one or multiple random members from a set.
   *
   * @param key
   *   Key of the set to remove items from
   * @param count
   *   Number of elements to remove
   * @return
   *   Returns the elements removed.
   */
  final def sPop[K: Schema](key: K, count: Option[Long] = None): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[R: Schema]: ZIO[Memcached, MemcachedError, Chunk[R]] = {
        val command = MemcachedCommand(
          SPop,
          Tuple2(ArbitraryInput[K](), OptionalInput(LongInput)),
          MultiStringChunkOutput(ArbitraryOutput[R]())
        )
        command.run((key, count))
      }
    }

  /**
   * Get one or multiple random members from a set.
   *
   * @param key
   *   Key of the set to get members from
   * @param count
   *   Number of elements to randomly get
   * @return
   *   Returns the random members.
   */
  final def sRandMember[K: Schema](key: K, count: Option[Long] = None): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[R: Schema]: ZIO[Memcached, MemcachedError, Chunk[R]] = {
        val command = MemcachedCommand(
          SRandMember,
          Tuple2(ArbitraryInput[K](), OptionalInput(LongInput)),
          MultiStringChunkOutput(ArbitraryOutput[R]())
        )
        command.run((key, count))
      }
    }

  /**
   * Remove one of more members from a set.
   *
   * @param key
   *   Key of the set to remove members from
   * @param member
   *   Value of the first element to remove
   * @param members
   *   Subsequent values of elements to remove
   * @return
   *   Returns the number of members that were removed from the set, not including non existing members.
   */
  final def sRem[K: Schema, M: Schema](key: K, member: M, members: M*): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(SRem, Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[M]())), LongOutput)
    command.run((key, (member, members.toList)))
  }

  /**
   * Incrementally iterate Set elements.
   *
   * This is a cursor based scan of an entire set. Call initially with cursor set to 0 and on subsequent calls pass the
   * return value as the next cursor.
   *
   * @param key
   *   Key of the set to scan
   * @param cursor
   *   Cursor to use for this iteration of scan
   * @param pattern
   *   Glob-style pattern that filters which elements are returned
   * @param count
   *   Count of elements. Roughly this number will be returned by Memcached if possible
   * @return
   *   Returns the next cursor, and items for this iteration or nothing when you reach the end, as a tuple.
   */
  final def sScan[K: Schema](
    key: K,
    cursor: Long,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ResultBuilder1[({ type lambda[x] = (Long, Chunk[x]) })#lambda] =
    new ResultBuilder1[({ type lambda[x] = (Long, Chunk[x]) })#lambda] {
      def returning[R: Schema]: ZIO[Memcached, MemcachedError, (Long, Chunk[R])] = {
        val command = MemcachedCommand(
          SScan,
          Tuple4(ArbitraryInput[K](), LongInput, OptionalInput(PatternInput), OptionalInput(CountInput)),
          Tuple2Output(MultiStringOutput.map(_.toLong), ChunkOutput(ArbitraryOutput[R]()))
        )
        command.run((key, cursor, pattern.map(Pattern(_)), count))
      }
    }

  /**
   * Add multiple sets.
   *
   * @param key
   *   Key of the first set to add
   * @param keys
   *   Keys of the subsequent sets to add
   * @return
   *   Returns a list with members of the resulting set.
   */
  final def sUnion[K: Schema](key: K, keys: K*): ResultBuilder1[Chunk] =
    new ResultBuilder1[Chunk] {
      def returning[R: Schema]: ZIO[Memcached, MemcachedError, Chunk[R]] =
        MemcachedCommand(SUnion, NonEmptyList(ArbitraryInput[K]()), ChunkOutput(ArbitraryOutput[R]()))
          .run((key, keys.toList))
    }

  /**
   * Add multiple sets and add the resulting set in a key.
   *
   * @param destination
   *   Key of destination to store the result
   * @param key
   *   Key of first set to add
   * @param keys
   *   Subsequent keys of sets to add
   * @return
   *   Returns the number of elements in the resulting set.
   */
  final def sUnionStore[D: Schema, K: Schema](
    destination: D,
    key: K,
    keys: K*
  ): ZIO[Memcached, MemcachedError, Long] = {
    val command = MemcachedCommand(SUnionStore, Tuple2(ArbitraryInput[D](), NonEmptyList(ArbitraryInput[K]())), LongOutput)
    command.run((destination, (key, keys.toList)))
  }
}

private[memcached] object Sets {
  val SAdd        = "SADD"
  val SCard       = "SCARD"
  val SDiff       = "SDIFF"
  val SDiffStore  = "SDIFFSTORE"
  val SInter      = "SINTER"
  val SInterStore = "SINTERSTORE"
  val SIsMember   = "SISMEMBER"
  val SMembers    = "SMEMBERS"
  val SMove       = "SMOVE"
  val SPop        = "SPOP"
  val SRandMember = "SRANDMEMBER"
  val SRem        = "SREM"
  val SScan       = "SSCAN"
  val SUnion      = "SUNION"
  val SUnionStore = "SUNIONSTORE"
}
