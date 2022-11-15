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

  final case class Numeric(value: String) extends RespValue

  final case class Array(values: Chunk[BulkStringWithHeader]) extends RespValue

  final case class BulkStringWithHeader(header: GenericValueHeader, value: Chunk[Byte]) extends RespValue

  final case class MetaResult(cd: RespValue, header: MetaValueHeader, value: Option[Chunk[Byte]]) extends RespValue

  final case class MetaDebugResult(header: Map[String, String]) extends RespValue

  case object End extends RespValue

  case object Stored extends RespValue

  case object NotStored extends RespValue

  case object Exists extends RespValue

  case object NotFound extends RespValue

  case object Touched extends RespValue

  case object Deleted extends RespValue

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
    final val ErrorRegex: Regex      = "^((?:CLIENT_ERROR|SERVER_ERROR|ERROR).*)$".r
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
                CollectingValue(bytes.toInt, ChunkBuilder.make[Byte](header.bytes), header)
              case ErrorRegex(err) =>
                Done(Error(err))
              case MetaValueRegex(command, flags) =>
                command match {
                  case "VA" =>
                    val space = flags.indexOf(' ')
                    if (space == -1) {
                      val bytes = flags.toInt
                      CollectingMetaValue(
                        bytes,
                        ChunkBuilder.make[Byte](bytes),
                        Map.empty
                      )
                    } else {
                      val bytes = flags.substring(0, space).toInt
                      CollectingMetaValue(
                        bytes,
                        ChunkBuilder.make[Byte](bytes),
                        parseMetaHeader(flags.substring(space + 1))
                      )
                    }
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
                Done(Numeric(value))
              case _ =>
                Failed
            }

          case CollectingValue(rem, chunk, header) =>
            if (rem == 0) {
              if (chars == EndChunk) {
                Done(BulkStringWithHeader(header, chunk.result()))
              } else {
                Failed
              }
            } else if (chars.length >= rem) {
              chunk ++= chars.take(rem)
              CollectingValue(
                0,
                chunk,
                header
              )
            } else {
              chunk ++= chars
              chunk ++= CrLfChunk
              CollectingValue(
                rem - chars.length - 2,
                chunk,
                header
              )
            }

          case CollectingMetaValue(rem, chunk, header) =>
            if (chars.length >= rem) {
              chunk ++= chars.take(rem)
              Done(MetaResult(Exists, header, Some(chunk.result())))
            } else {
              chunk ++= chars
              chunk ++= CrLfChunk
              CollectingMetaValue(rem - chars.length - 2, chunk, header)
            }

          case _ => Failed
        }
    }

    object State {
      case object Start extends State

      case object Failed extends State

      final case class CollectingValue(
        rem: Int,
        chunk: ChunkBuilder[Byte],
        collectedHeaders: GenericValueHeader
      ) extends State

      final case class CollectingMetaValue(rem: Int, chunk: ChunkBuilder[Byte], header: MetaValueHeader) extends State

      final case class Done(value: RespValue) extends State
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
