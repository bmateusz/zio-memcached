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

import zio.memcached.model.CasUnique
import zio.memcached.model.MetaResult._
import zio.memcached.model.UpdateResult.{Exists, NotFound, UpdateResult, Updated}
import zio.memcached.model.ValueHeaders.{MetaValueHeader, ValueHeaderWithCas}
import zio.schema.Schema
import zio.schema.codec.Codec

sealed trait Output[+A] {
  self =>

  private[memcached] final def unsafeDecode(respValue: RespValue)(implicit codec: Codec): A =
    respValue match {
      case RespValue.Error(msg) =>
        throw MemcachedError.ProtocolError(msg)
      case success =>
        tryDecode(success)
    }

  protected def tryDecode(respValue: RespValue)(implicit codec: Codec): A

  final def map[B](f: A => B): Output[B] =
    new Output[B] {
      protected def tryDecode(respValue: RespValue)(implicit codec: Codec): B = f(self.tryDecode(respValue))
    }

}

object Output {

  import MemcachedError._

  def apply[A](implicit output: Output[A]): Output[A] = output

  final case class SingleGetOutput[A]()(implicit schema: Schema[A]) extends Output[Option[A]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Option[A] =
      respValue match {
        case RespValue.End => None
        case RespValue.BulkStringWithHeader(h, RespValue.BulkString(s)) =>
          codec.decode(schema)(s).fold(e => throw CodecError(e), Some(_))
        case other => throw ProtocolError(s"$other isn't a SingleGetOutput")
      }
  }

  final case class SingleGetWithCasOutput[A]()(implicit schema: Schema[A]) extends Output[Option[(CasUnique, A)]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Option[(CasUnique, A)] =
      respValue match {
        case RespValue.End => None
        case RespValue.BulkStringWithHeader(
              ValueHeaderWithCas(_, _, _, casUnique: CasUnique),
              RespValue.BulkString(s)
            ) =>
          codec.decode(schema)(s).fold(e => throw CodecError(e), Some(_)).map((casUnique, _))
        case other => throw ProtocolError(s"$other isn't a SingleGetWithCasOutput")
      }
  }

  final object TouchOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Boolean =
      respValue match {
        case RespValue.Touched  => true
        case RespValue.NotFound => false
        case other              => throw ProtocolError(s"$other isn't a TouchOutput")
      }
  }

  case object SetOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Boolean =
      respValue match {
        case RespValue.Stored    => true
        case RespValue.NotStored => false
        case other               => throw ProtocolError(s"$other isn't a SetOutput")
      }
  }

  case object UpdateResultOutput extends Output[UpdateResult] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): UpdateResult =
      respValue match {
        case RespValue.Stored   => Updated
        case RespValue.Exists   => Exists
        case RespValue.NotFound => NotFound
        case other              => throw ProtocolError(s"$other isn't a UpdateResultOutput")
      }
  }

  case object NumericOutput extends Output[Long] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Long =
      respValue match {
        case RespValue.Numeric(i) => i
        case other                => throw ProtocolError(s"$other isn't a NumericOutput")
      }
  }

  case object DeleteOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Boolean =
      respValue match {
        case RespValue.Deleted  => true
        case RespValue.NotFound => false
        case other              => throw ProtocolError(s"$other isn't a DeleteOutput")
      }
  }

  final case class MetaGetOutput[A]()(implicit schema: Schema[A]) extends Output[MetaGetResult[A]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): MetaGetResult[A] =
      respValue match {
        case RespValue.MetaResult(RespValue.NotFound, _, _) =>
          MetaGetResultNotFound()
        case RespValue.MetaResult(cd: RespValue, h: MetaValueHeader, Some(RespValue.BulkString(s))) =>
          codec.decode(schema)(s).fold(e => throw CodecError(e), MetaGetResultSingle(_, h))
        case RespValue.MetaResult(cd: RespValue, h: MetaValueHeader, None) =>
          MetaGetResultFlagsOnly(h)
        case other => throw ProtocolError(s"$other isn't a MetaGetOutput")
      }
  }

  final case object MetaSetOutput extends Output[MetaSetResult] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): MetaSetResult =
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

  final case object MetaDeleteOutput extends Output[MetaDeleteResult] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): MetaDeleteResult =
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

  final case object MetaArithmeticOutput extends Output[MetaArithmeticResult] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): MetaArithmeticResult =
      respValue match {
        case RespValue.MetaResult(RespValue.Stored, h: MetaValueHeader, value) =>
          MetaArithmeticResultSuccess(h, value.map(_.asLong))
        case RespValue.MetaResult(RespValue.Exists, h: MetaValueHeader, value) =>
          MetaArithmeticResultExists(h, value.map(_.asLong))
        case RespValue.MetaResult(RespValue.NotFound, h: MetaValueHeader, None) =>
          MetaArithmeticResultNotFound(h)
        case RespValue.MetaResult(RespValue.NotStored, h: MetaValueHeader, None) =>
          MetaArithmeticResultNotStored(h)
        case other => throw ProtocolError(s"$other isn't a MetaArithmeticOutput")
      }
  }

  final case object MetaDebugOutput extends Output[Option[Map[String, String]]] {
    protected def tryDecode(respValue: RespValue)(implicit codec: Codec): Option[Map[String, String]] =
      respValue match {
        case RespValue.MetaDebugResult(value) =>
          Some(value)
        case RespValue.MetaResult(RespValue.NotFound, _, _) =>
          None
        case other => throw ProtocolError(s"$other isn't a MetaDebugOutput")
      }
  }

}
