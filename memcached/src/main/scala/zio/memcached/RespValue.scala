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
import zio.memcached.model.ValueHeaders.{GenericValueHeader, MetaValueHeader, valueHeader}
import zio.stream._

import scala.util.matching.Regex

sealed trait RespValue

object RespValue {
  final case class Error(value: String) extends RespValue

  final case class Numeric(value: Long) extends RespValue

  final case class BulkString(value: Chunk[Byte]) extends RespValue {
    def length: Int = value.length

    private[memcached] def asString: String = new String(value.toArray)

    private[memcached] def asLong: Long = internal.unsafeReadLong(asString, 0)

    private[memcached] def serialize: Chunk[Byte] =
      value ++ internal.CrLfChunk
  }

  final case class Array(values: Chunk[BulkStringWithHeader]) extends RespValue

  final case class BulkStringWithHeader(header: GenericValueHeader, value: BulkString) extends RespValue

  final case class MetaResult(cd: RespValue, header: MetaValueHeader, value: Option[BulkString]) extends RespValue

  final case class MetaDebugResult(header: Map[String, String]) extends RespValue

  final case object End extends RespValue

  final case object Stored extends RespValue

  final case object NotStored extends RespValue

  final case object Exists extends RespValue

  final case object NotFound extends RespValue

  final case object Touched extends RespValue

  final case object Deleted extends RespValue

  private[memcached] final val decoder = {
    import internal.State

    // ZSink fold will return a State.Start when contFn is false
    val lineProcessor =
      ZSink.fold[Chunk[Byte], State](State.Start)(_.inProgress)(_ feed _).mapZIO {
        case State.Done(value) => ZIO.succeedNow(Some(value))
        case State.Failed      => ZIO.fail(MemcachedError.ProtocolError("Invalid data received."))
        case State.Start       => ZIO.succeedNow(None)
        case other             => ZIO.dieMessage(s"Deserialization bug, should not get $other")
      }

    ZPipeline
      .splitOnChunk(internal.CrLfChunk)
      .andThen(ZPipeline.mapChunks((chars: Chunk[Byte]) => Chunk.single(chars)))
      .andThen(ZPipeline.fromSink(lineProcessor))
  }

  private object internal {
    final val CrLfChunk: Chunk[Byte] = Chunk('\r', '\n')
    final val EndChunk: Chunk[Byte]  = Chunk('E', 'N', 'D')
    final val ValueRegex: Regex      = "^VALUE (\\S+) (\\d+) (\\d+) ?(\\d+)?$".r
    final val MetaValueRegex: Regex  = "^([a-zA-Z]{2})\\s?(.+)*$".r
    final val ErrorRegex: Regex      = "^(?:CLIENT_|SERVER_)?ERROR.*$".r
    final val NumericRegex: Regex    = "^\\s*(\\d+)\\s*$".r
    final val KeyValueRegex: Regex   = "^(\\S+)=(\\S+)$".r

    sealed trait State {
      self =>

      import State._

      final def inProgress: Boolean =
        self match {
          case Done(_) | Failed => false
          case _                => true
        }

      final def feed(chars: Chunk[Byte]): State =
        self match {
          case Start =>
            new String(chars.toArray) match {
              case ""           => Start
              case "END"        => Done(End)
              case "STORED"     => Done(Stored)
              case "NOT_STORED" => Done(NotStored)
              case "EXISTS"     => Done(Exists)
              case "NOT_FOUND"  => Done(NotFound)
              case "TOUCHED"    => Done(Touched)
              case "DELETED"    => Done(Deleted)
              case ValueRegex(key, flags, bytes, cas) =>
                val header = valueHeader(key, flags, bytes, cas)
                CollectingValue(bytes.toInt, Chunk.empty, Chunk(header), Chunk.empty)
              case MetaValueRegex(command, flags) =>
                command match {
                  case "VA" =>
                    val space = flags.indexOf(' ')
                    if (space == -1)
                      CollectingMetaValue(flags.toInt, Chunk.empty, Map.empty)
                    else
                      CollectingMetaValue(
                        flags.substring(0, space).toInt,
                        Chunk.empty,
                        parseMetaHeader(flags.substring(space + 1))
                      )
                  case "HD" => // Means header only, indicates success
                    Done(MetaResult(Stored, parseMetaHeader(flags), None))
                  case "EN" | "NF" =>
                    Done(MetaResult(NotFound, parseMetaHeader(flags), None))
                  case "NS" =>
                    Done(MetaResult(NotStored, parseMetaHeader(flags), None))
                  case "EX" =>
                    Done(MetaResult(Exists, parseMetaHeader(flags), None))
                  case "ME" =>
                    Done(MetaDebugResult(parseMetaDebugHeader(flags)))
                  case "MN" => // Answer of Meta No-Op
                    Done(MetaResult(End, parseMetaHeader(flags), None))
                  case _ =>
                    Failed
                }
              case NumericRegex(value) =>
                Done(Numeric(value.toLongOption.getOrElse(0L)))
              case err @ ErrorRegex() =>
                Done(Error(err))
              case _ =>
                Failed
            }

          case CollectingValue(rem, chunk, collectedHeaders, collectedValues) =>
            if (rem == 0) {
              if (chars == EndChunk) {
                collectedHeaders.zip(collectedValues) match {
                  case Chunk()      => Failed
                  case Chunk(value) => Done(BulkStringWithHeader.tupled(value))
                  case values       => Done(Array(values.map(BulkStringWithHeader.tupled)))
                }
              } else {
                new String(chars.toArray) match {
                  case ValueRegex(key, flags, bytes, cas) =>
                    val header = valueHeader(key, flags, bytes, cas)
                    CollectingValue(
                      bytes.toInt,
                      Chunk.empty,
                      collectedHeaders :+ header,
                      collectedValues
                    )
                  case _ =>
                    Failed
                }
              }
            } else if (chars.length >= rem) {
              val finalChunk = chunk ++ chars.take(rem)
              CollectingValue(
                0,
                finalChunk,
                collectedHeaders,
                collectedValues :+ BulkString(finalChunk)
              )
            } else {
              CollectingValue(
                rem - chars.length - 2,
                chunk ++ chars ++ CrLfChunk,
                collectedHeaders,
                collectedValues
              )
            }

          case CollectingMetaValue(rem, chunk, header) =>
            if (chars.length >= rem) {
              val finalChunk = chunk ++ chars.take(rem)
              Done(MetaResult(Exists, header, Some(BulkString(finalChunk))))
            } else {
              CollectingMetaValue(rem - chars.length - 2, chunk ++ chars ++ CrLfChunk, header)
            }

          case _ => Failed
        }
    }

    object State {
      case object Start extends State

      case object Failed extends State

      final case class CollectingValue(
        rem: Int,
        chunk: Chunk[Byte],
        collectedHeaders: Chunk[GenericValueHeader],
        collectedValues: Chunk[BulkString]
      ) extends State

      final case class CollectingMetaValue(rem: Int, chunk: Chunk[Byte], header: MetaValueHeader) extends State

      final case class Done(value: RespValue) extends State
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

    private def parseMetaHeader(string: String): MetaValueHeader =
      if (string == null) {
        Map.empty
      } else {
        string
          .split(" ")
          .map { flag =>
            val key   = flag.charAt(0)
            val value = flag.substring(1)
            key -> value
          }
          .toMap
      }

    private def parseMetaDebugHeader(string: String): Map[String, String] =
      if (string == null) {
        Map.empty
      } else {
        val flags = string.split(" ")
        flags.map {
          case KeyValueRegex(key, value) => key   -> value
          case other                     => "key" -> other // the first value in the output is the key
        }.toMap
      }
  }

}
