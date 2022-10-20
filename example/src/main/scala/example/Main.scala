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

import example.api.Api
import example.config.AppConfig
import zhttp.service.Server
import zio._
import zio.memcached.{MemcachedExecutor, MemcachedLive}
import zio.schema.codec.{Codec, ProtobufCodec}

object Main extends ZIOAppDefault {
  def run: ZIO[ZIOAppArgs with Scope, Any, ExitCode] =
    Server
      .start(9000, Api.routes)
      .provide(
        AppConfig.layer,
        MemcachedApiLive.layer,
        MemcachedExecutor.layer,
        MemcachedLive.layer,
        ZLayer.succeed[Codec](ProtobufCodec)
      )
      .exitCode
}
