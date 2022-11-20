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

package zio.memcached.codec

import zio._
import zio.memcached._
import zio.schema.codec.BinaryCodec
import zio.test.Assertion.{exists => _, _}
import zio.test._

object StringUtf8CodecSpec extends BaseSpec {

  override implicit val codec: BinaryCodec = StringUtf8Codec

  def spec: Spec[TestEnvironment with Scope, MemcachedError] =
    suite("storage")(
      suite("set and get")(
        test("ascii string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- get[String](key)
          } yield assert(result)(isSome(equalTo("value")))
        },
        test("emtpy string") {
          for {
            key    <- uuid
            result <- get[String](key)
          } yield assert(result)(isNone)
        },
        test("string including CrLf") {
          for {
            key    <- uuid
            _      <- set(key, "carriage return\r\nline feed")
            result <- get[String](key)
          } yield assert(result)(isSome(equalTo("carriage return\r\nline feed")))
        },
        test("latin2 string") {
          for {
            key    <- uuid
            _      <- set(key, "Árvíztűrő tükörfúrógép")
            result <- get[String](key)
          } yield assert(result)(isSome(equalTo("Árvíztűrő tükörfúrógép")))
        },
        test("unicode string") {
          for {
            key    <- uuid
            _      <- set(key, "日本国")
            result <- get[String](key)
          } yield assert(result)(isSome(equalTo("日本国")))
        },
        test("emoji string") {
          for {
            key    <- uuid
            _      <- set(key, "\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67\u200D\uD83D\uDC66")
            result <- get[String](key)
          } yield assert(result)(isSome(equalTo("\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67\u200D\uD83D\uDC66")))
        },
        test("int") {
          for {
            key    <- uuid
            _      <- set(key, 1)
            result <- get[Int](key)
          } yield assert(result)(isSome(equalTo(1)))
        },
        test("long") {
          for {
            key    <- uuid
            _      <- set(key, Long.MaxValue)
            result <- get[Long](key)
          } yield assert(result)(isSome(equalTo(9223372036854775807L: Long)))
        },
        test("double") {
          for {
            key    <- uuid
            _      <- set(key, 1.0)
            result <- get[Double](key)
          } yield assert(result)(isSome(equalTo(1.0)))
        }
      )
    ).provideLayerShared(TestLayer)

  private val TestLayer =
    ZLayer.make[Memcached](MemcachedExecutor.test, MemcachedLive.layer, ZLayer.succeed(codec))
}
