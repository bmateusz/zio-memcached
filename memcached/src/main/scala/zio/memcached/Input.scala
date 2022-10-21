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
import zio.memcached.RespValue.BulkString
import zio.memcached.model.{CasUnique, MetaArithmeticFlags, MetaDebugFlags, MetaDeleteFlags, MetaGetFlags, MetaSetFlags}
import zio.schema.Schema
import zio.schema.codec.Codec

import java.nio.charset.StandardCharsets
import java.time.Instant

sealed trait Input[-A] {
  self =>

  private[memcached] def encode(data: A)(implicit codec: Codec): Chunk[BulkString]
}

object Input {

  private[memcached] val EmptyChunk: Chunk[Byte] = Chunk.empty
  private[memcached] val WhitespaceChunk         = Chunk.fromArray(" ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val SpaceZeroSpaceChunk     = Chunk.fromArray(" 0 ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val GetWsChunk              = Chunk.fromArray("get ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val GetsWsChunk             = Chunk.fromArray("gets ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val TouchWsChunk            = Chunk.fromArray("touch ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val GatWsChunk              = Chunk.fromArray("gat ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val GatsWsChunk             = Chunk.fromArray("gats ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val SetWsChunk              = Chunk.fromArray("set ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val AddWsChunk              = Chunk.fromArray("add ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val AppendWsChunk           = Chunk.fromArray("append ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val PrependWsChunk          = Chunk.fromArray("prepend ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val ReplaceWsChunk          = Chunk.fromArray("replace ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val CasWsChunk              = Chunk.fromArray("cas ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val IncrWsChunk             = Chunk.fromArray("incr ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val DecrWsChunk             = Chunk.fromArray("decr ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val DeleteWsChunk           = Chunk.fromArray("delete ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val MgWsChunk               = Chunk.fromArray("mg ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val MsWsChunk               = Chunk.fromArray("ms ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val MdWsChunk               = Chunk.fromArray("md ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val MaWsChunk               = Chunk.fromArray("ma ".getBytes(StandardCharsets.US_ASCII))
  private[memcached] val MeWsChunk               = Chunk.fromArray("me ".getBytes(StandardCharsets.US_ASCII))

  def apply[A](implicit input: Input[A]): Input[A] = input

  @inline
  private[this] def encodeBytes[A](value: A)(implicit codec: Codec, schema: Schema[A]): BulkString =
    BulkString(codec.encode(schema)(value))

  private val ThirtyDaysInSeconds: Long = 60 * 60 * 24 * 30

  private[this] def durationToSeconds(duration: Duration): Long =
    duration match {
      case Duration.Infinity =>
        0L
      case finite: Duration =>
        val seconds = finite.getSeconds

        if (seconds > ThirtyDaysInSeconds)
          Instant.now().plusSeconds(seconds).getEpochSecond
        else
          seconds
    }

  final object GetCommand extends Input[String] {
    def encode(data: String)(implicit codec: Codec): Chunk[BulkString] =
      Chunk.single(BulkString(GetWsChunk ++ data.getBytes(StandardCharsets.US_ASCII)))
  }

  final object GetsCommand extends Input[String] {
    def encode(data: String)(implicit codec: Codec): Chunk[BulkString] =
      Chunk.single(BulkString(GetsWsChunk ++ data.getBytes(StandardCharsets.US_ASCII)))
  }

  final object TouchCommand extends Input[(String, Duration)] {
    def encode(data: (String, Duration))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, expireTime) = data
      val expireSeconds     = durationToSeconds(expireTime)
      Chunk.single(
        BulkString(
          TouchWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            expireSeconds.toString.getBytes(StandardCharsets.US_ASCII)
        )
      )
    }
  }

  final object GatCommand extends Input[(String, Duration)] {
    def encode(data: (String, Duration))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, expireTime) = data
      val expireSeconds     = durationToSeconds(expireTime)
      Chunk.single(
        BulkString(
          GatWsChunk ++ expireSeconds.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            key.getBytes(StandardCharsets.US_ASCII)
        )
      )
    }
  }

  final object GatsCommand extends Input[(String, Duration)] {
    def encode(data: (String, Duration))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, expireTime) = data
      val expireSeconds     = durationToSeconds(expireTime)
      Chunk.single(
        BulkString(
          GatsWsChunk ++ expireSeconds.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            key.getBytes(StandardCharsets.US_ASCII)
        )
      )
    }
  }

  abstract class GenericSetCommand[A: Schema]() extends Input[(String, A, Option[Duration])] {
    protected val operationWsChunk: Chunk[Byte]

    def encode(data: (String, A, Option[Duration]))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, value, expireTime) = data
      val expireSeconds            = expireTime.map(durationToSeconds).getOrElse(0L)
      val encodedValue             = encodeBytes(value)
      Chunk(
        BulkString(
          operationWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ SpaceZeroSpaceChunk ++
            expireSeconds.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            encodedValue.length.toString.getBytes(StandardCharsets.US_ASCII)
        ),
        encodedValue
      )
    }
  }

  final class SetCommand[A: Schema]() extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = SetWsChunk
  }

  final class AddCommand[A: Schema]() extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = AddWsChunk
  }

  final class ReplaceCommand[A: Schema]() extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = ReplaceWsChunk
  }

  final class AppendCommand[A: Schema]() extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = AppendWsChunk
  }

  final class PrependCommand[A: Schema]() extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = PrependWsChunk
  }

  final class CompareAndSetCommand[A: Schema]() extends Input[(String, A, CasUnique, Option[Duration])] {
    def encode(data: (String, A, CasUnique, Option[Duration]))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, value, casUnique, expireTime) = data
      val expireSeconds = expireTime match {
        case Some(value: Duration) => value.getSeconds
        case None                  => 0L
      }
      val encodedValue = encodeBytes(value)
      Chunk(
        BulkString(
          CasWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ SpaceZeroSpaceChunk ++
            expireSeconds.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            encodedValue.length.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            casUnique.value.toString.getBytes(StandardCharsets.US_ASCII)
        ),
        encodedValue
      )
    }
  }

  final object IncreaseCommand extends Input[(String, Long)] {
    def encode(data: (String, Long))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, value) = data
      Chunk.single(
        BulkString(
          IncrWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            value.toString.getBytes(StandardCharsets.US_ASCII)
        )
      )
    }
  }

  final object DecreaseCommand extends Input[(String, Long)] {
    def encode(data: (String, Long))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, value) = data
      Chunk.single(
        BulkString(
          DecrWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            value.toString.getBytes(StandardCharsets.US_ASCII)
        )
      )
    }
  }

  final object DeleteCommand extends Input[String] {
    def encode(data: String)(implicit codec: Codec): Chunk[BulkString] =
      Chunk.single(BulkString(DeleteWsChunk ++ data.getBytes(StandardCharsets.US_ASCII)))
  }

  final object MetaGetCommand extends Input[(String, MetaGetFlags)] {
    def encode(data: (String, MetaGetFlags))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, flags) = data
      Chunk.single(BulkString(MgWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ flags.encoded))
    }
  }

  final class MetaSetCommand[A: Schema]() extends Input[(String, A, MetaSetFlags)] {
    def encode(data: (String, A, MetaSetFlags))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, value, flags) = data
      val encodedValue        = encodeBytes(value)
      val valueLength         = encodedValue.length.toString.getBytes(StandardCharsets.US_ASCII)
      Chunk(
        BulkString(
          MsWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++ valueLength ++ flags.encoded
        ),
        encodedValue
      )
    }
  }

  final object MetaDeleteCommand extends Input[(String, MetaDeleteFlags)] {
    def encode(data: (String, MetaDeleteFlags))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, flags) = data
      Chunk.single(BulkString(MdWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ flags.encoded))
    }
  }

  final object MetaArithmeticCommand extends Input[(String, MetaArithmeticFlags)] {
    def encode(data: (String, MetaArithmeticFlags))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, flags) = data
      Chunk.single(BulkString(MaWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ flags.encoded))
    }
  }

  final object MetaDebugCommand extends Input[(String, MetaDebugFlags)] {
    def encode(data: (String, MetaDebugFlags))(implicit codec: Codec): Chunk[BulkString] = {
      val (key, flags) = data
      Chunk.single(BulkString(MeWsChunk ++ key.getBytes(StandardCharsets.US_ASCII) ++ flags.encoded))
    }
  }

}
