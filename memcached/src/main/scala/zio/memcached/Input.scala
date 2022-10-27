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
import zio.memcached.model.{CasUnique, MetaArithmeticFlags, MetaDebugFlags, MetaDeleteFlags, MetaGetFlags, MetaSetFlags}
import zio.schema.Schema
import zio.schema.codec.Codec

import java.time.Instant

object Input {
  type Input = Chunk[Byte]

  private[memcached] val CrLfChunk = Chunk.fromArray("\r\n".getBytes())

  @inline
  private[this] def encodeBytes[A](value: A)(implicit codec: Codec, schema: Schema[A]): Chunk[Byte] =
    codec.encode(schema)(value)

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

  object GetCommand {
    def apply(key: String): Input =
      Chunk.fromArray(s"get $key\r\n".getBytes())
  }

  object GetsCommand {
    def apply(key: String): Input =
      Chunk.fromArray(s"gets $key\r\n".getBytes())
  }

  object TouchCommand {
    def apply(key: String, duration: Duration): Input = {
      val expires = durationToSeconds(duration)
      Chunk.fromArray(s"touch $key $expires\r\n".getBytes())
    }
  }

  object GatCommand {
    def apply(key: String, duration: Duration): Input = {
      val expires = durationToSeconds(duration)
      Chunk.fromArray(s"gat $expires $key\r\n".getBytes())
    }
  }

  object GatsCommand {
    def apply(key: String, duration: Duration): Input = {
      val expires = durationToSeconds(duration)
      Chunk.fromArray(s"gats $expires $key\r\n".getBytes())
    }
  }

  object GenericSetCommand {
    def apply[A: Schema](
      command: String,
      key: String,
      expireTime: Option[Duration],
      value: A
    )(implicit codec: Codec): Input = {
      val expires      = expireTime.map(durationToSeconds).getOrElse(0L)
      val encodedValue = encodeBytes(value)
      val header       = s"$command $key 0 $expires ${encodedValue.length}\r\n".getBytes()
      GenericSetCommand(Chunk.fromArray(header), encodedValue)
    }

    def apply[A: Schema](
      command: String,
      key: String,
      expireTime: Option[Duration],
      cas: CasUnique,
      value: A
    )(implicit codec: Codec): Input = {
      val expires      = expireTime.map(durationToSeconds).getOrElse(0L)
      val encodedValue = encodeBytes(value)
      val header       = s"$command $key 0 $expires ${encodedValue.length} ${cas.value}\r\n".getBytes()
      GenericSetCommand(Chunk.fromArray(header), encodedValue)
    }

    def apply(header: Chunk[Byte], value: Chunk[Byte]): Chunk[Byte] = {
      val builder = ChunkBuilder.make[Byte](header.length + value.length + 2)
      builder ++= header
      builder ++= value
      builder ++= CrLfChunk
      builder.result()
    }
  }

  object SetCommand {
    def apply[A: Schema](key: String, expireTime: Option[Duration], value: A)(implicit codec: Codec): Input =
      GenericSetCommand("set", key, expireTime, value)
  }

  object AddCommand {
    def apply[A: Schema](key: String, expireTime: Option[Duration], value: A)(implicit codec: Codec): Input =
      GenericSetCommand("add", key, expireTime, value)
  }

  object ReplaceCommand {
    def apply[A: Schema](key: String, expireTime: Option[Duration], value: A)(implicit codec: Codec): Input =
      GenericSetCommand("replace", key, expireTime, value)
  }

  object AppendCommand {
    def apply[A: Schema](key: String, expireTime: Option[Duration], value: A)(implicit codec: Codec): Input =
      GenericSetCommand("append", key, expireTime, value)
  }

  object PrependCommand {
    def apply[A: Schema](key: String, expireTime: Option[Duration], value: A)(implicit codec: Codec): Input =
      GenericSetCommand("prepend", key, expireTime, value)
  }

  object CompareAndSetCommand {
    def apply[A: Schema](key: String, casUnique: CasUnique, expireTime: Option[Duration], value: A)(implicit
      codec: Codec
    ): Input =
      GenericSetCommand("cas", key, expireTime, casUnique, value)
  }

  object IncrementCommand {
    def apply(key: String, value: Long): Input =
      Chunk.fromArray(s"incr $key $value\r\n".getBytes())
  }

  object DecrementCommand {
    def apply(key: String, value: Long): Input =
      Chunk.fromArray(s"decr $key $value\r\n".getBytes())
  }

  object DeleteCommand {
    def apply(key: String): Input =
      Chunk.fromArray(s"delete $key\r\n".getBytes())
  }

  object MetaGetCommand {
    def apply(key: String, flags: MetaGetFlags): Input =
      Chunk.fromArray(s"mg $key${flags.encoded}\r\n".getBytes())
  }

  object MetaSetCommand {
    def apply[A: Schema](key: String, value: A, flags: MetaSetFlags)(implicit codec: Codec): Input = {
      val encodedValue = encodeBytes(value)
      val header       = s"ms $key ${encodedValue.length}${flags.encoded}\r\n".getBytes()
      GenericSetCommand(Chunk.fromArray(header), encodedValue)
    }
  }

  object MetaDeleteCommand {
    def apply(key: String, flags: MetaDeleteFlags): Input =
      Chunk.fromArray(s"md $key${flags.encoded}\r\n".getBytes())
  }

  object MetaArithmeticCommand {
    def apply(key: String, flags: MetaArithmeticFlags): Input =
      Chunk.fromArray(s"ma $key${flags.encoded}\r\n".getBytes())
  }

  object MetaDebugCommand {
    def apply(key: String, flags: MetaDebugFlags): Input =
      Chunk.fromArray(s"me $key${flags.encoded}\r\n".getBytes())
  }
}
