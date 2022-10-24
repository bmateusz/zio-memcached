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
import zio.memcached.Input.EncodedCommand
import zio.memcached.RespValue.BulkString
import zio.memcached.model.{CasUnique, MetaArithmeticFlags, MetaDebugFlags, MetaDeleteFlags, MetaGetFlags, MetaSetFlags}
import zio.schema.Schema
import zio.schema.codec.Codec

import java.nio.charset.StandardCharsets
import java.time.Instant

sealed trait Input[-A] {
  self =>

  private[memcached] def encode(data: A)(implicit codec: Codec): EncodedCommand

  val key: String

  def keyChunk: Chunk[Byte] =
    Chunk.fromArray(key.getBytes(StandardCharsets.US_ASCII))
}

object Input {

  sealed trait EncodedCommand {
    val chunk: Chunk[Byte]

    val command: BulkString

    private[memcached] def asString: String = command.asString
  }

  case class Command(override val command: BulkString) extends EncodedCommand {
    override val chunk: Chunk[Byte] = command.serialize
  }

  case class CommandAndValue(override val command: BulkString, value: BulkString) extends EncodedCommand {
    override val chunk: Chunk[Byte] = command.serialize ++ value.serialize
  }

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

  @inline
  private[this] def encodeBytes[A](value: A)(implicit codec: Codec, schema: Schema[A]): BulkString =
    BulkString(codec.encode(schema)(value))

  val ThirtyDaysInSeconds: Long = 60 * 60 * 24 * 30

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

  final class GetCommand(override val key: String) extends Input[Unit] {
    def encode(unit: Unit)(implicit codec: Codec): EncodedCommand =
      Command(BulkString(GetWsChunk ++ keyChunk))
  }

  final class GetsCommand(override val key: String) extends Input[Unit] {
    def encode(unit: Unit)(implicit codec: Codec): EncodedCommand =
      Command(BulkString(GetsWsChunk ++ keyChunk))
  }

  final class TouchCommand(override val key: String) extends Input[Duration] {
    def encode(expireTime: Duration)(implicit codec: Codec): EncodedCommand = {
      val expireSeconds = durationToSeconds(expireTime)
      Command(
        BulkString(
          TouchWsChunk ++ keyChunk ++ WhitespaceChunk ++
            expireSeconds.toString.getBytes(StandardCharsets.US_ASCII)
        )
      )
    }
  }

  final class GatCommand(override val key: String) extends Input[Duration] {
    def encode(expireTime: Duration)(implicit codec: Codec): EncodedCommand = {
      val expireSeconds = durationToSeconds(expireTime)
      Command(
        BulkString(
          GatWsChunk ++ expireSeconds.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++ keyChunk
        )
      )
    }
  }

  final class GatsCommand(override val key: String) extends Input[Duration] {
    def encode(expireTime: Duration)(implicit codec: Codec): EncodedCommand = {
      val expireSeconds = durationToSeconds(expireTime)
      Command(
        BulkString(
          GatsWsChunk ++ expireSeconds.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            keyChunk
        )
      )
    }
  }

  abstract class GenericSetCommand[A: Schema]() extends Input[(A, Option[Duration])] {
    protected val operationWsChunk: Chunk[Byte]

    def encode(data: (A, Option[Duration]))(implicit codec: Codec): EncodedCommand = {
      val (value, expireTime) = data
      val expireSeconds       = expireTime.map(durationToSeconds).getOrElse(0L)
      val encodedValue        = encodeBytes(value)
      CommandAndValue(
        BulkString(
          operationWsChunk ++ keyChunk ++ SpaceZeroSpaceChunk ++
            expireSeconds.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            encodedValue.length.toString.getBytes(StandardCharsets.US_ASCII)
        ),
        encodedValue
      )
    }
  }

  final class SetCommand[A: Schema](override val key: String) extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = SetWsChunk
  }

  final class AddCommand[A: Schema](override val key: String) extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = AddWsChunk
  }

  final class ReplaceCommand[A: Schema](override val key: String) extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = ReplaceWsChunk
  }

  final class AppendCommand[A: Schema](override val key: String) extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = AppendWsChunk
  }

  final class PrependCommand[A: Schema](override val key: String) extends GenericSetCommand[A] {
    override protected val operationWsChunk: Chunk[Byte] = PrependWsChunk
  }

  final class CompareAndSetCommand[A: Schema](override val key: String)
      extends Input[(A, CasUnique, Option[Duration])] {
    def encode(data: (A, CasUnique, Option[Duration]))(implicit codec: Codec): EncodedCommand = {
      val (value, casUnique, expireTime) = data
      val expireSeconds = expireTime match {
        case Some(value: Duration) => value.getSeconds
        case None                  => 0L
      }
      val encodedValue = encodeBytes(value)
      CommandAndValue(
        BulkString(
          CasWsChunk ++ keyChunk ++ SpaceZeroSpaceChunk ++
            expireSeconds.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            encodedValue.length.toString.getBytes(StandardCharsets.US_ASCII) ++ WhitespaceChunk ++
            casUnique.value.toString.getBytes(StandardCharsets.US_ASCII)
        ),
        encodedValue
      )
    }
  }

  final class IncrementCommand(override val key: String) extends Input[Long] {
    def encode(value: Long)(implicit codec: Codec): EncodedCommand =
      Command(
        BulkString(
          IncrWsChunk ++ keyChunk ++ WhitespaceChunk ++
            value.toString.getBytes(StandardCharsets.US_ASCII)
        )
      )
  }

  final class DecrementCommand(override val key: String) extends Input[Long] {
    def encode(value: Long)(implicit codec: Codec): EncodedCommand =
      Command(
        BulkString(
          DecrWsChunk ++ keyChunk ++ WhitespaceChunk ++
            value.toString.getBytes(StandardCharsets.US_ASCII)
        )
      )
  }

  final class DeleteCommand(override val key: String) extends Input[Unit] {
    def encode(unit: Unit)(implicit codec: Codec): EncodedCommand =
      Command(BulkString(DeleteWsChunk ++ keyChunk))
  }

  final class MetaGetCommand(override val key: String) extends Input[MetaGetFlags] {
    def encode(flags: MetaGetFlags)(implicit codec: Codec): EncodedCommand =
      Command(BulkString(MgWsChunk ++ keyChunk ++ flags.encoded))
  }

  final class MetaSetCommand[A: Schema](override val key: String) extends Input[(A, MetaSetFlags)] {
    def encode(data: (A, MetaSetFlags))(implicit codec: Codec): EncodedCommand = {
      val (value, flags) = data
      val encodedValue   = encodeBytes(value)
      val valueLength    = encodedValue.length.toString.getBytes(StandardCharsets.US_ASCII)
      CommandAndValue(
        BulkString(
          MsWsChunk ++ keyChunk ++ WhitespaceChunk ++ valueLength ++ flags.encoded
        ),
        encodedValue
      )
    }
  }

  final class MetaDeleteCommand(override val key: String) extends Input[MetaDeleteFlags] {
    def encode(flags: MetaDeleteFlags)(implicit codec: Codec): EncodedCommand =
      Command(BulkString(MdWsChunk ++ keyChunk ++ flags.encoded))
  }

  final class MetaArithmeticCommand(override val key: String) extends Input[MetaArithmeticFlags] {
    def encode(flags: MetaArithmeticFlags)(implicit codec: Codec): EncodedCommand =
      Command(BulkString(MaWsChunk ++ keyChunk ++ flags.encoded))
  }

  final class MetaDebugCommand(override val key: String) extends Input[MetaDebugFlags] {
    def encode(flags: MetaDebugFlags)(implicit codec: Codec): EncodedCommand =
      Command(BulkString(MeWsChunk ++ keyChunk ++ flags.encoded))
  }

}
