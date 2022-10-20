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

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.matching.Regex

sealed trait RespValue

object RespValue {
  final case class Error(value: String) extends RespValue

  final case class Numeric(value: Long) extends RespValue

  final case class BulkString(value: Chunk[Byte]) extends RespValue {
    def length: Int = value.length

    private[memcached] def asString: String = decode(value)

    private[memcached] def asLong: Long = internal.unsafeReadLong(asString, 0)

    private[memcached] def serialize: Chunk[Byte] =
      value ++ internal.CrLf
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

  private[memcached] def decode(bytes: Chunk[Byte]): String = new String(bytes.toArray, StandardCharsets.UTF_8)

  private object internal {
    final val CrLf: Chunk[Byte]     = Chunk('\r', '\n')
    final val CrLfString: String    = "\r\n"
    final val ValueRegex: Regex     = "^VALUE (\\S+) (\\d+) (\\d+) ?(\\d+)?$".r
    final val MetaValueRegex: Regex = "^([a-zA-Z]{2})\\s?(\\d+)?\\s?(.+)*$".r
    final val ErrorRegex: Regex     = "^(?:CLIENT_|SERVER_)?ERROR.*$".r
    final val NumericRegex: Regex   = "^\\s*(\\d+)\\s*$".r
    final val KeyValueRegex: Regex  = "^(\\S+)=(\\S+)$".r

    sealed trait State {
      self =>

      import State._

      final def inProgress: Boolean =
        self match {
          case Done(_) | Failed => false
          case _                => true
        }

      final def feed(line: String): State =
        self match {
          case Start =>
            line match {
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
                CollectingValue(bytes.toInt, new mutable.StringBuilder(), Chunk(header), Chunk.empty)
              case MetaValueRegex(command, bytes, flags) =>
                command match {
                  case "VA" =>
                    CollectingMetaValue(bytes.toInt, new mutable.StringBuilder(), parseMetaHeader(flags))
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
                Done(Numeric(value.toLong))
              case ErrorRegex() =>
                Done(Error(line))
              case _ =>
                Failed
            }

          case CollectingValue(rem, stringBuilder, collectedHeaders, collectedValues) =>
            if (rem == 0) {
              if (line == "END") {
                collectedHeaders.zip(collectedValues) match {
                  case Chunk()      => Failed
                  case Chunk(value) => Done(BulkStringWithHeader.tupled(value))
                  case values       => Done(Array(values.map(BulkStringWithHeader.tupled)))
                }
              } else {
                line match {
                  case ValueRegex(key, flags, bytes, cas) =>
                    val header = valueHeader(key, flags, bytes, cas)
                    CollectingValue(
                      bytes.toInt,
                      new mutable.StringBuilder(),
                      collectedHeaders :+ header,
                      collectedValues
                    )
                  case _ =>
                    Failed
                }
              }
            } else if (line.length >= rem) {
              val stringValue = stringBuilder.append(line.substring(0, rem)).toString
              CollectingValue(
                0,
                stringBuilder,
                collectedHeaders,
                collectedValues :+ BulkString(Chunk.fromArray(stringValue.getBytes(StandardCharsets.UTF_8)))
              )
            } else {
              CollectingValue(
                rem - line.length - 2,
                stringBuilder.append(line).append(CrLfString),
                collectedHeaders,
                collectedValues
              )
            }

          case CollectingMetaValue(rem, stringBuilder, header) =>
            if (line.length >= rem) {
              val stringValue = stringBuilder.append(line.substring(0, rem)).toString
              Done(
                MetaResult(
                  Exists,
                  header,
                  Some(BulkString(Chunk.fromArray(stringValue.getBytes(StandardCharsets.UTF_8))))
                )
              )
            } else {
              CollectingMetaValue(rem - line.length - 2, stringBuilder.append(line).append(CrLfString), header)
            }

          case _ => Failed
        }
    }

    object State {
      case object Start extends State

      case object Failed extends State

      final case class CollectingValue(
        rem: Int,
        stringBuilder: mutable.StringBuilder,
        collectedHeaders: Chunk[GenericValueHeader],
        collectedValues: Chunk[BulkString]
      ) extends State

      final case class CollectingMetaValue(rem: Int, stringBuilder: mutable.StringBuilder, header: MetaValueHeader)
          extends State

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
