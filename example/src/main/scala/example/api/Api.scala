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

package example.api

import example._
import zio._
import zio.http._
import zio.http.model.{Method, Status}
import zio.json._
import zio.memcached.Memcached
import zio.stream.ZStream

object Api {
  val memcachedApi: HttpApp[Memcached with MemcachedApi, Throwable] =
    Http.collectZIO { case req @ Method.POST -> !! / "memcached" / command =>
      req.body.asString
        .map(_.fromJson[MemcachedRequest])
        .flatMap {
          case Left(value) =>
            ZIO.succeed(Response.text(s"Bad input $value").setStatus(Status.BadRequest))
          case Right(body) =>
            ZIO
              .serviceWithZIO[MemcachedApi](_.executeCommand(command, body))
              .tapError(err => ZIO.logError(s"Error executing command $command with body $body: $err"))
              .mapBoth(_.toResponse, r => Response.text(r))
              .merge
              .catchAllCause { a =>
                ZIO.logError(a.toString).as(Response.text(a.toString).setStatus(Status.InternalServerError))
              }
        }
    }

  val static: UHttpApp =
    Http.collectHttp[Request] { case Method.GET -> !! =>
      Http.fromStream(ZStream.fromResource("index.html"))
    }

  val routes: HttpApp[Memcached with MemcachedApi, Throwable] =
    memcachedApi ++ static

}
