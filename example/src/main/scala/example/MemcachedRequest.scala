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

package example

import zio._
import zio.json.{DeriveJsonCodec, JsonCodec}
import zio.memcached.model.CasUnique

final case class MemcachedRequest(
  key: String,
  value: Option[String],
  ttl: Option[String],
  compareAndSet: Option[String],
  metaFlags: Option[String]
) {
  def ttoAsDuration: Option[Duration] = ttl.flatMap(_.toIntOption).map(_.seconds)

  def extractValue: ZIO[Any, ApiError, String] =
    ZIO.fromOption(value).mapError(_ => ApiError.MissingMandatoryField("value"))

  def extractLongValue: ZIO[Any, ApiError, Long] =
    ZIO.fromOption(value.flatMap(_.toLongOption)).mapError(_ => ApiError.MissingMandatoryField("value (numeric)"))

  def extractTtl: ZIO[Any, ApiError, Duration] =
    ZIO.fromOption(ttoAsDuration).mapError(_ => ApiError.MissingMandatoryField("ttl"))

  def extractCas: ZIO[Any, ApiError, CasUnique] =
    ZIO
      .fromOption(compareAndSet.flatMap(_.toLongOption))
      .mapBoth(_ => ApiError.MissingMandatoryField("compareAndSet"), CasUnique.apply)

  def extractMetaFlags: ZIO[Any, ApiError, String] =
    ZIO.fromOption(metaFlags).mapError(_ => ApiError.MissingMandatoryField("metaFlags"))
}

object MemcachedRequest {
  implicit val codec: JsonCodec[MemcachedRequest] = DeriveJsonCodec.gen[MemcachedRequest]
}