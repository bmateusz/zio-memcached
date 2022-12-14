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
import zio.memcached._
import zio.memcached.model.{MetaArithmeticFlags, MetaDebugFlags, MetaDeleteFlags, MetaGetFlags, MetaSetFlags}

import scala.util.chaining.scalaUtilChainingOps

trait MemcachedApi {
  def executeCommand(command: String, body: MemcachedRequest): ZIO[Memcached, ApiError, String]
}

final case class MemcachedApiLive(r: Memcached) extends MemcachedApi {
  def executeCommand(command: String, body: MemcachedRequest): ZIO[Memcached, ApiError, String] =
    (command match {
      case "set" =>
        body.extractValue.flatMap { value =>
          set(body.key, value, body.ttlAsDuration).map(_.toString)
        }
      case "get" =>
        get[String](body.key).pipe(handleGetOption)
      case "getWithCas" =>
        getWithCas[String](body.key).pipe(handleGetOption)
      case "touch" =>
        body.extractTtl.flatMap { ttl =>
          touch(body.key, ttl).map(_.toString)
        }
      case "getAndTouch" =>
        body.extractTtl.flatMap { ttl =>
          getAndTouch[String](body.key, ttl).pipe(handleGetOption)
        }
      case "getAndTouchWithCas" =>
        body.extractTtl.flatMap { ttl =>
          getAndTouchWithCas[String](body.key, ttl).pipe(handleGetOption)
        }
      case "delete" =>
        delete(body.key).map(_.toString)
      case "increase" =>
        body.extractLongValue.flatMap { value =>
          increment(body.key, value).map(_.toString)
        }
      case "add" =>
        body.extractValue.flatMap { value =>
          add(body.key, value, body.ttlAsDuration).map(_.toString)
        }
      case "replace" =>
        body.extractValue.flatMap { value =>
          replace(body.key, value, body.ttlAsDuration).map(_.toString)
        }
      case "append" =>
        body.extractValue.flatMap { value =>
          append(body.key, value).map(_.toString)
        }
      case "prepend" =>
        body.extractValue.flatMap { value =>
          prepend(body.key, value).map(_.toString)
        }
      case "compareAndSwap" =>
        body.extractValue.flatMap { value =>
          body.extractCas.flatMap { cas =>
            compareAndSwap(body.key, value, cas, body.ttlAsDuration).map(_.toString)
          }
        }
      case "metaGet" =>
        body.extractMetaFlags.flatMap { flags =>
          metaGet[String](body.key, MetaGetFlags.fromString(flags)).map(_.toString)
        }
      case "metaSet" =>
        body.extractValue.flatMap { value =>
          body.extractMetaFlags.flatMap { flags =>
            metaSet(body.key, value, MetaSetFlags.fromString(flags)).map(_.toString)
          }
        }
      case "metaDelete" =>
        body.extractMetaFlags.flatMap { flags =>
          metaDelete(body.key, MetaDeleteFlags.fromString(flags)).map(_.toString)
        }
      case "metaArithmetic" =>
        body.extractMetaFlags.flatMap { flags =>
          metaArithmetic(body.key, MetaArithmeticFlags.fromString(flags)).map(_.toString)
        }
      case "metaDebug" =>
        body.extractMetaFlags.flatMap { flags =>
          metaDebug(body.key, MetaDebugFlags.fromString(flags)).map(_.toString)
        }
      case _ =>
        ZIO.fail(ApiError.CommandNotFound)
    }).refineOrDie {
      case m: MemcachedError => ApiError.MemcachedError(m)
      case a: ApiError       => a
    }

  private def handleGetOption[A](
    zio: ZIO[Memcached, MemcachedError, Option[A]]
  ): ZIO[Memcached, MemcachedError, String] =
    zio.map(_.map(_.toString).getOrElse("Cache miss"))
}

object MemcachedApiLive {
  lazy val layer: URLayer[Memcached, MemcachedApi] =
    ZLayer.fromFunction(MemcachedApiLive.apply _)
}
