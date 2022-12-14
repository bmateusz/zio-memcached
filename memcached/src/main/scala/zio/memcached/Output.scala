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

import zio.memcached.model.MetaResult._
import zio.memcached.model.UpdateResult.{Exists, NotFound, UpdateResult, Updated}
import zio.memcached.model.ValueHeaders.{MetaValueHeader, ValueHeaderWithCas}
import zio.memcached.model.{CasUnique, ValueWithCasUnique}
import zio.schema.Schema
import zio.schema.codec.BinaryCodec

sealed trait Output[+A] {
  self =>

  private[memcached] final def unsafeDecode(respValue: RespValue)(implicit codec: BinaryCodec): A =
    tryDecode(respValue)

  protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): A

  final def map[B](f: A => B): Output[B] =
    new Output[B] {
      protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): B = f(self.tryDecode(respValue))
    }

}

object Output {

  import MemcachedError._

  def apply[A](implicit output: Output[A]): Output[A] = output

  final case class SingleGetOutput[A]()(implicit schema: Schema[A]) extends Output[Option[A]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): Option[A] =
      respValue match {
        case RespValue.End => None
        case RespValue.BulkStringWithHeader(_, s) =>
          codec.decode(schema)(s).fold(e => throw e, Some(_))
        case other => throw ProtocolError(s"$other isn't a SingleGetOutput")
      }
  }

  final case class SingleGetWithCasOutput[A]()(implicit schema: Schema[A])
      extends Output[Option[ValueWithCasUnique[A]]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): Option[ValueWithCasUnique[A]] =
      respValue match {
        case RespValue.End => None
        case RespValue.BulkStringWithHeader(ValueHeaderWithCas(_, _, _, casUnique: CasUnique), s) =>
          codec.decode(schema)(s).fold(e => throw e, Some(_)).map(new ValueWithCasUnique(_, casUnique))
        case other => throw ProtocolError(s"$other isn't a SingleGetWithCasOutput")
      }
  }

  case object TouchOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): Boolean =
      respValue match {
        case RespValue.Touched  => true
        case RespValue.NotFound => false
        case other              => throw ProtocolError(s"$other isn't a TouchOutput")
      }
  }

  case object SetOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): Boolean =
      respValue match {
        case RespValue.Stored    => true
        case RespValue.NotStored => false
        case other               => throw ProtocolError(s"$other isn't a SetOutput")
      }
  }

  case object UpdateResultOutput extends Output[UpdateResult] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): UpdateResult =
      respValue match {
        case RespValue.Stored   => Updated
        case RespValue.Exists   => Exists
        case RespValue.NotFound => NotFound
        case other              => throw ProtocolError(s"$other isn't a UpdateResultOutput")
      }
  }

  case object NumericOutput extends Output[Option[Long]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): Option[Long] =
      respValue match {
        case RespValue.Numeric(i)                    => Some(i.toLong)
        case RespValue.Error(_) | RespValue.NotFound => None
        case other                                   => throw ProtocolError(s"$other isn't a NumericOutput")
      }
  }

  case object DeleteOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): Boolean =
      respValue match {
        case RespValue.Deleted  => true
        case RespValue.NotFound => false
        case other              => throw ProtocolError(s"$other isn't a DeleteOutput")
      }
  }

  final case class MetaGetOutput[A]()(implicit schema: Schema[A]) extends Output[MetaGetResult[A]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): MetaGetResult[A] =
      respValue match {
        case RespValue.MetaResult(RespValue.NotFound, _, _) =>
          MetaGetResultNotFound()
        case RespValue.MetaResult(_: RespValue, h: MetaValueHeader, Some(s)) =>
          codec.decode(schema)(s).fold(e => throw e, MetaGetResultValue(_, h))
        case RespValue.MetaResult(_: RespValue, h: MetaValueHeader, None) =>
          MetaGetResultHeadersOnly(h)
        case other => throw ProtocolError(s"$other isn't a MetaGetOutput")
      }
  }

  case object MetaSetOutput extends Output[MetaSetResult] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): MetaSetResult =
      respValue match {
        case RespValue.MetaResult(RespValue.Stored, h: MetaValueHeader, None) =>
          MetaSetResultStored(h)
        case RespValue.MetaResult(RespValue.NotStored, h: MetaValueHeader, None) =>
          MetaSetResultNotStored(h)
        case RespValue.MetaResult(RespValue.Exists, h: MetaValueHeader, None) =>
          MetaSetResultExists(h)
        case RespValue.MetaResult(RespValue.NotFound, h: MetaValueHeader, None) =>
          MetaSetResultNotFound(h)
        case other => throw ProtocolError(s"$other isn't a MetaSetOutput")
      }
  }

  case object MetaDeleteOutput extends Output[MetaDeleteResult] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): MetaDeleteResult =
      respValue match {
        case RespValue.MetaResult(RespValue.Stored, h: MetaValueHeader, None) =>
          MetaDeleteResultDeleted(h)
        case RespValue.MetaResult(RespValue.Exists, h: MetaValueHeader, None) =>
          MetaDeleteResultExists(h)
        case RespValue.MetaResult(RespValue.NotFound, h: MetaValueHeader, None) =>
          MetaDeleteResultNotFound(h)
        case other => throw ProtocolError(s"$other isn't a MetaDeleteOutput")
      }
  }

  case object MetaArithmeticOutput extends Output[MetaArithmeticResult] {
    import zio.Chunk

    def chunkBytesToLong(bytes: Chunk[Byte]): Long =
      new String(bytes.toArray).toLong

    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): MetaArithmeticResult =
      respValue match {
        case RespValue.MetaResult(RespValue.Stored, h: MetaValueHeader, value) =>
          MetaArithmeticResultSuccess(h, value.map(chunkBytesToLong))
        case RespValue.MetaResult(RespValue.Exists, h: MetaValueHeader, value) =>
          MetaArithmeticResultExists(h, value.map(chunkBytesToLong))
        case RespValue.MetaResult(RespValue.NotFound, h: MetaValueHeader, None) =>
          MetaArithmeticResultNotFound(h)
        case RespValue.MetaResult(RespValue.NotStored, h: MetaValueHeader, None) =>
          MetaArithmeticResultNotStored(h)
        case other => throw ProtocolError(s"$other isn't a MetaArithmeticOutput")
      }
  }

  case object MetaDebugOutput extends Output[MetaDebugResult] {
    protected def tryDecode(respValue: RespValue)(implicit codec: BinaryCodec): MetaDebugResult =
      respValue match {
        case RespValue.MetaDebugResult(value) =>
          MetaDebugResultSuccess(value)
        case RespValue.MetaResult(RespValue.NotFound, _, _) =>
          MetaDebugResultNotFound
        case other => throw ProtocolError(s"$other isn't a MetaDebugOutput")
      }
  }

}
