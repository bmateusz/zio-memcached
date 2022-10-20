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
import zhttp.http._
import zio._
import zio.json._
import zio.memcached.Memcached
import zio.stream.ZStream

object Api {
  // Read the file as ZStream
  // val content = HttpData.fromStream(ZStream.fromFile("example/src/main/resources/index.html")) {
  // val indexFromResource = HttpData.fromStream(ZStream.fromResource("index.html"))

  val contributors: HttpApp[ContributorsCache, Nothing] =
    Http.collectZIO {
      case Method.GET -> !! / "repositories" / owner / name / "contributors" =>
        ZIO
          .serviceWithZIO[ContributorsCache](_.fetchAll(Repository(Owner(owner), Name(name))))
          .mapBoth(_.toResponse, r => Response.json(r.toJson))
          .merge
    }

  val memcachedApi: HttpApp[Memcached with MemcachedApi, Throwable] =
    Http.collectZIO {
      case req@Method.POST -> !! / "memcached" / command =>
        req.bodyAsString.flatMap { body => // TODO Handle Task[String] and get rid of Throwable error type
          body.fromJson[MemcachedRequest] match {
            case Left(value) =>
              ZIO.succeed(Response.text(s"Bad input $value").setStatus(Status.BadRequest))
            case Right(body) =>
              ZIO
                .serviceWithZIO[MemcachedApi](_.executeCommand(command, body))
                .mapBoth(_.toResponse, r => Response.text(r))
                .merge
          }
        }
    }

  val static: UHttpApp =
    Http.collectHttp[Request] {
      case Method.GET -> !! =>
        Http.fromStream(ZStream.fromResource("index.html"))
    }

  val routes: HttpApp[ContributorsCache with Memcached with MemcachedApi, Throwable] =
    memcachedApi ++ contributors ++ static

}
