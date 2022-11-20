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
import zio.memcached.BaseSpec.Person
import zio.memcached.model.UpdateResult
import zio.schema.DeriveSchema.gen
import zio.schema.Schema
import zio.test.Assertion.{exists => _, _}
import zio.test._

trait StorageSpec extends BaseSpec {
  private def setGetAndAssert[A](value: A)(implicit schema: Schema[A]) =
    for {
      key    <- uuid
      _      <- set(key, value)
      result <- get[A](key)
    } yield assert(result)(isSome(equalTo(value)))

  def storageSuite: Spec[Memcached, MemcachedError] =
    suite("storage")(
      suite("set and get")(
        test("get nothing") {
          for {
            key    <- uuid
            result <- get[String](key)
          } yield assert(result)(isNone)
        },
        test("ascii string") {
          check(Gen.asciiString)(setGetAndAssert(_))
        },
        test("unicode string") {
          check(Gen.string)(setGetAndAssert(_))
        },
        test("byte") {
          check(Gen.byte)(setGetAndAssert(_))
        },
        test("short") {
          check(Gen.short)(setGetAndAssert(_))
        },
        test("int") {
          check(Gen.int)(setGetAndAssert(_))
        },
        test("char") {
          check(Gen.unicodeChar)(setGetAndAssert(_))
        },
        test("long") {
          check(Gen.long)(setGetAndAssert(_))
        },
        test("boolean") {
          check(Gen.boolean)(setGetAndAssert(_))
        },
        test("float") {
          check(Gen.float)(setGetAndAssert(_))
        },
        test("double") {
          check(Gen.double)(setGetAndAssert(_))
        },
        test("big int") {
          check(Gen.bigInt(minBigInt, maxBigInt))(setGetAndAssert(_))
        },
        test("big decimal") {
          check(Gen.bigDecimal(minBigDecimal, maxBigDecimal))(setGetAndAssert(_))
        },
        test("option string") {
          check(Gen.option(Gen.string))(setGetAndAssert(_))
        },
        test("either string or long") {
          check(Gen.either(Gen.string, Gen.long))(setGetAndAssert(_))
        },
        test("list of strings") {
          check(Gen.listOf(Gen.string))(setGetAndAssert(_))
        },
        test("vector of strings") {
          check(Gen.vectorOf(Gen.string))(setGetAndAssert(_))
        },
        test("set of strings") {
          check(Gen.setOf(Gen.string))(setGetAndAssert(_))
        },
        test("map") {
          check(Gen.mapOf(Gen.string, Gen.long))(setGetAndAssert(_))
        },
        test("tuple2") {
          check(Gen.string <*> Gen.string)(setGetAndAssert(_))
        },
        test("byte array chunk") {
          check(Gen.chunkOf(Gen.byte))(setGetAndAssert(_))
        },
        test("uuid") {
          check(Gen.uuid)(setGetAndAssert(_))
        },
        test("case class") {
          check(Gen.int)(i => setGetAndAssert(Person("John", i)))
        }
      ),
      suite("delete")(
        test("delete existing key") {
          for {
            key       <- uuid
            _         <- set(key, "value")
            delResult <- delete(key)
            getResult <- get[String](key)
          } yield assert(getResult)(isNone) && assert(delResult)(isTrue)
        },
        test("delete non-existing key") {
          for {
            key    <- uuid
            result <- delete(key)
          } yield assert(result)(isFalse)
        }
      ),
      suite("time to live")(
        test("set ttl") {
          for {
            key    <- uuid
            _      <- set(key, "value", Some(1.day))
            result <- get[String](key)
          } yield assert(result)(isSome(equalTo("value")))
        },
        test("set ttl longer than 30 days") {
          for {
            key    <- uuid
            _      <- set(key, "value", Some(90.days))
            result <- get[String](key)
          } yield assert(result)(isSome(equalTo("value")))
        },
        test("set ttl and expire") {
          for {
            key     <- uuid
            _       <- set(key, "value", Some(1.second))
            delayed <- get[String](key).delay(2.seconds).fork
            result  <- TestClock.adjust(3.seconds) *> delayed.join
          } yield assert(result)(isNone)
        },
        test("set ttl and update") {
          for {
            key    <- uuid
            _      <- set(key, "value", Some(1.second))
            _      <- set(key, "value2")
            result <- get[String](key)
          } yield assert(result)(isSome(equalTo("value2")))
        },
        test("set ttl and delete") {
          for {
            key    <- uuid
            _      <- set(key, "value", Some(1.second))
            _      <- delete(key)
            result <- get[String](key)
          } yield assert(result)(isNone)
        },
        test("set ttl with touch") {
          for {
            key     <- uuid
            _       <- set(key, "value", None)
            _       <- touch(key, 1.second)
            delayed <- get[String](key).delay(2.seconds).fork
            result  <- TestClock.adjust(3.seconds) *> delayed.join
          } yield assert(result)(isNone)
        },
        test("set ttl with get and touch") {
          for {
            key       <- uuid
            _         <- set(key, "value", None)
            gatResult <- getAndTouch[String](key, 1.seconds)
            delayed   <- get[String](key).delay(2.seconds).fork
            getResult <- TestClock.adjust(3.seconds) *> delayed.join
          } yield assert(gatResult)(isSome(equalTo("value"))) && assert(getResult)(isNone)
        }
      ),
      suite("special set operations")(
        test("add (set if not exists) success") {
          for {
            key       <- uuid
            _         <- set(key, "value")
            addResult <- add(key, "value2")
            getResult <- get[String](key)
          } yield assert(addResult)(isFalse) && assert(getResult)(isSome(equalTo("value")))
        },
        test("add (set if not exists) failure") {
          for {
            key       <- uuid
            addResult <- add(key, "value2")
            getResult <- get[String](key)
          } yield assert(addResult)(isTrue) && assert(getResult)(isSome(equalTo("value2")))
        },
        test("replace (only set if exists) success") {
          for {
            key           <- uuid
            _             <- set(key, "value")
            replaceResult <- replace(key, "value2")
            getResult     <- get[String](key)
          } yield assert(replaceResult)(isTrue) && assert(getResult)(isSome(equalTo("value2")))
        },
        test("replace (only set if exists) failure") {
          for {
            key           <- uuid
            replaceResult <- replace(key, "value2")
            getResult     <- get[String](key)
          } yield assert(replaceResult)(isFalse) && assert(getResult)(isNone)
        },
        test("append success") {
          for {
            key          <- uuid
            _            <- set(key, "value")
            appendResult <- append(key, "---")
            getResult    <- get[String](key)
          } yield assert(appendResult)(isTrue) && assert(getResult)(isSome(equalTo("value---")))
        },
        test("append failure") {
          for {
            key          <- uuid
            appendResult <- append(key, "---")
            getResult    <- get[String](key)
          } yield assert(appendResult)(isFalse) && assert(getResult)(isNone)
        },
        test("prepend success") {
          for {
            key           <- uuid
            _             <- set(key, "value")
            prependResult <- prepend(key, "---")
            getResult     <- get[String](key)
          } yield assert(prependResult)(isTrue) && assert(getResult)(isSome(equalTo("---value")))
        },
        test("prepend failure") {
          for {
            key           <- uuid
            prependResult <- prepend(key, "---")
            getResult     <- get[String](key)
          } yield assert(prependResult)(isFalse) && assert(getResult)(isNone)
        }
      ),
      suite("compare and swap")(
        test("cas updated") {
          for {
            key             <- uuid
            _               <- set(key, "value")
            optValueWithCas <- getWithCas[String](key)
            casRes          <- compareAndSwap(key, "value2", optValueWithCas.get.casUnique)
            getRes          <- get[String](key)
          } yield assert(casRes)(equalTo(UpdateResult.Updated)) && assert(getRes)(isSome(equalTo("value2")))
        },
        test("cas exists (not updated)") {
          for {
            key             <- uuid
            _               <- set(key, "value")
            optValueWithCas <- getAndTouchWithCas[String](key, 1.day)
            _               <- set(key, "value2")
            casRes          <- compareAndSwap(key, "value3", optValueWithCas.get.casUnique)
            getRes          <- get[String](key)
          } yield assert(casRes)(equalTo(UpdateResult.Exists)) && assert(getRes)(isSome(equalTo("value2")))
        },
        test("cas not exists") {
          for {
            key             <- uuid
            _               <- set(key, "value")
            optValueWithCas <- getWithCas[String](key)
            _               <- delete(key)
            casRes          <- compareAndSwap(key, "value2", optValueWithCas.get.casUnique)
            getRes          <- get[String](key)
          } yield assert(casRes)(equalTo(UpdateResult.NotFound)) && assert(getRes)(isNone)
        }
      ),
      suite("increment and decrement")(
        test("increment number in string representation") {
          for {
            key    <- uuid
            _      <- set(key, "1")
            incRes <- increment(key, 1)
            getRes <- get[String](key)
          } yield assert(incRes)(isSome(equalTo(2L))) && assert(getRes)(isSome(equalTo("2")))
        },
        test("decrement number in string representation") {
          for {
            key    <- uuid
            _      <- set(key, "6")
            incRes <- increment(key, -1)
            getRes <- get[String](key)
          } yield assert(incRes)(isSome(equalTo(5L))) && assert(getRes)(isSome(equalTo("5")))
        },
        test("decrement number underflow in string representation") {
          for {
            key    <- uuid
            _      <- set(key, "5")
            incRes <- increment(key, -6)
            getRes <- get[String](key)
          } yield assert(incRes)(isSome(equalTo(0L))) && assert(getRes)(isSome(equalTo("0")))
        },
        test("increment number in binary representation (results in error)") {
          for {
            key    <- uuid
            _      <- set(key, 1L)
            incRes <- increment(key, 1)
          } yield assert(incRes)(isNone)
        },
        test("increment non existing key") {
          for {
            key    <- uuid
            incRes <- increment(key, 1L)
          } yield assert(incRes)(isNone)
        }
      ),
      suite("key validation")(
        test("invalid key (with space)") {
          for {
            exit <- get[String]("invalid key").exit
          } yield assert(exit)(fails(equalTo(MemcachedError.InvalidKey)))
        },
        test("invalid key (with control character)") {
          for {
            exit <- get[String]("invalid\u0000key").exit
          } yield assert(exit)(fails(equalTo(MemcachedError.InvalidKey)))
        },
        test("invalid key (empty)") {
          for {
            exit <- get[String]("").exit
          } yield assert(exit)(fails(equalTo(MemcachedError.InvalidKey)))
        },
        test("invalid key (too long)") {
          for {
            exit <- get[String]("a" * 251).exit
          } yield assert(exit)(fails(equalTo(MemcachedError.InvalidKey)))
        },
        test("valid key (with non-ascii character)") {
          for {
            key    <- uuid.map(_ + "-valid\u00A0key")
            result <- get[String](key)
          } yield assert(result)(isNone)
        }
      ),
      suite("interruption")(
        test("immediate interruption handled") {
          for {
            key    <- uuid
            s      <- set(key, "value").fork
            _      <- s.interrupt
            result <- get[String](key)
          } yield assert(result)(isNone)
        } @@ TestAspect.flaky
      )
    )
}
