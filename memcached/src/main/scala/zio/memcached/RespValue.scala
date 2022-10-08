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
import zio.stream._

import java.nio.charset.StandardCharsets

sealed trait RespValue extends Product with Serializable { self =>
  import RespValue._
  import RespValue.internal.{CrLf, Headers, NullArrayEncoded, NullStringEncoded}

  final def serialize: Chunk[Byte] =
    self match {
      case NullBulkString  => NullStringEncoded
      case NullArray       => NullArrayEncoded
      case SimpleString(s) => Headers.SimpleString +: encode(s)
      case Error(s)        => Headers.Error +: encode(s)
      case Integer(i)      => Headers.Integer +: encode(i.toString)

      case BulkString(bytes) =>
        Headers.BulkString +: (encode(bytes.length.toString) ++ bytes ++ CrLf)

      case Array(elements) =>
        val data = elements.foldLeft[Chunk[Byte]](Chunk.empty)(_ ++ _.serialize)
        Headers.Array +: (encode(elements.size.toString) ++ data)
    }

  private[this] def encode(s: String): Chunk[Byte] =
    Chunk.fromArray(s.getBytes(StandardCharsets.US_ASCII)) ++ CrLf
}

object RespValue {
  final case class SimpleString(value: String) extends RespValue

  final case class Error(value: String) extends RespValue

  final case class Integer(value: Long) extends RespValue

  final case class BulkString(value: Chunk[Byte]) extends RespValue {
    private[memcached] def asString: String = decode(value)

    private[memcached] def asLong: Long = internal.unsafeReadLong(asString, 0)
  }

  final case class Array(values: Chunk[RespValue]) extends RespValue

  case object NullBulkString extends RespValue

  case object NullArray extends RespValue

  object ArrayValues {
    def unapplySeq(v: RespValue): Option[Seq[RespValue]] =
      v match {
        case Array(values) => Some(values)
        case _             => None
      }
  }

  private[memcached] final val decoder = {
    import internal.State

    // ZSink fold will return a State.Start when contFn is false
    val lineProcessor =
      ZSink.fold[String, State](State.Start)(_.inProgress)(_ feed _).mapZIO {
        case State.Done(value) => ZIO.succeedNow(Some(value))
        case State.Failed      => ZIO.fail(MemcachedError.ProtocolError("Invalid data received."))
        case State.Start       => ZIO.succeedNow(None)
        case other             => ZIO.dieMessage(s"Deserialization bug, should not get $other")
      }

    (ZPipeline.utf8Decode >>> ZPipeline.splitOn(internal.CrLfString))
      .mapError(e => MemcachedError.ProtocolError(e.getLocalizedMessage))
      .andThen(ZPipeline.fromSink(lineProcessor))
  }

  private[memcached] def array(values: RespValue*): Array = Array(Chunk.fromIterable(values))

  private[memcached] def bulkString(s: String): BulkString = BulkString(Chunk.fromArray(s.getBytes(StandardCharsets.UTF_8)))

  private[memcached] def decode(bytes: Chunk[Byte]): String = new String(bytes.toArray, StandardCharsets.UTF_8)

  private object internal {
    object Headers {
      final val SimpleString: Byte = '+'
      final val Error: Byte        = '-'
      final val Integer: Byte      = ':'
      final val BulkString: Byte   = '$'
      final val Array: Byte        = '*'
    }

    final val CrLf: Chunk[Byte]              = Chunk('\r', '\n')
    final val CrLfString: String             = "\r\n"
    final val NullArrayEncoded: Chunk[Byte]  = Chunk.fromArray("*-1\r\n".getBytes(StandardCharsets.US_ASCII))
    final val NullArrayPrefix: String        = "*-1"
    final val NullStringEncoded: Chunk[Byte] = Chunk.fromArray("$-1\r\n".getBytes(StandardCharsets.US_ASCII))
    final val NullStringPrefix: String       = "$-1"

    sealed trait State { self =>
      import State._

      final def inProgress: Boolean =
        self match {
          case Done(_) | Failed => false
          case _                => true
        }

      final def feed(line: String): State =
        self match {
          case Start if line.isEmpty()           => Start
          case Start if line == NullStringPrefix => Done(NullBulkString)
          case Start if line == NullArrayPrefix  => Done(NullArray)

          case Start if line.nonEmpty =>
            line.head match {
              case Headers.SimpleString => Done(SimpleString(line.tail))
              case Headers.Error        => Done(Error(line.tail))
              case Headers.Integer      => Done(Integer(unsafeReadLong(line, 1)))
              case Headers.BulkString =>
                val size = unsafeReadLong(line, 1).toInt
                CollectingBulkString(size, new StringBuilder(size))
              case Headers.Array =>
                val size = unsafeReadLong(line, 1).toInt

                if (size > 0)
                  CollectingArray(size, ChunkBuilder.make(size), Start.feed)
                else
                  Done(Array(Chunk.empty))

              case _ => Failed
            }

          case CollectingArray(rem, vals, next) =>
            next(line) match {
              case Done(v) if rem > 1 => CollectingArray(rem - 1, vals += v, Start.feed)
              case Done(v)            => Done(Array((vals += v).result()))
              case state              => CollectingArray(rem, vals, state.feed)
            }

          case CollectingBulkString(rem, vals) =>
            if (line.length >= rem) {
              val stringValue = vals.append(line.substring(0, rem)).toString
              Done(BulkString(Chunk.fromArray(stringValue.getBytes(StandardCharsets.UTF_8))))
            } else {
              CollectingBulkString(rem - line.length - 2, vals.append(line).append(CrLfString))
            }

          case _ => Failed
        }
    }

    object State {
      case object Start                                                                                extends State
      case object Failed                                                                               extends State
      final case class CollectingArray(rem: Int, vals: ChunkBuilder[RespValue], next: String => State) extends State
      final case class CollectingBulkString(rem: Int, vals: StringBuilder)                             extends State
      final case class Done(value: RespValue)                                                          extends State
    }

    def unsafeReadLong(text: String, startFrom: Int): Long = {
      var pos = startFrom
      var res = 0L
      var neg = false

      if (text.charAt(pos) == '-') {
        neg = true
        pos += 1
      }

      val len = text.length

      while (pos < len) {
        res = res * 10 + text.charAt(pos) - '0'
        pos += 1
      }

      if (neg) -res else res
    }
  }
}
