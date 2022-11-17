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
import zio.memcached.model.MetaResult._
import zio.memcached.model.{MetaArithmeticFlags, MetaDeleteFlags, MetaGetFlags, MetaSetFlags}
import zio.schema.DeriveSchema.gen
import zio.test.Assertion.{exists => _, _}
import zio.test._

import java.nio.charset.StandardCharsets
import java.util.Base64

trait MetaSpec extends BaseSpec {
  def metaSuite: Spec[Memcached, MemcachedError] =
    suite("meta")(
      suite("set and get")(
        test("ascii string") {
          for {
            key    <- uuid
            _      <- metaSet(key, "value")
            result <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue("value", Map.empty)))
        },
        test("string with CrLf") {
          for {
            key    <- uuid
            _      <- metaSet(key, "value\r\n")
            result <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue("value\r\n", Map.empty)))
        },
        test("unicode string") {
          for {
            key    <- uuid
            _      <- metaSet(key, "ąęćżźńłó")
            result <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue("ąęćżźńłó", Map.empty)))
        },
        test("byte") {
          for {
            key    <- uuid
            _      <- metaSet(key, 1.toByte)
            result <- metaGet[Byte](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(1.toByte, Map.empty)))
        },
        test("short") {
          for {
            key    <- uuid
            _      <- metaSet(key, 1.toShort)
            result <- metaGet[Short](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(1.toShort, Map.empty)))
        },
        test("int") {
          for {
            key    <- uuid
            _      <- metaSet(key, 1)
            result <- metaGet[Int](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(1, Map.empty)))
        },
        test("char") {
          for {
            key    <- uuid
            _      <- metaSet(key, 'a')
            result <- metaGet[Char](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue('a', Map.empty)))
        },
        test("long") {
          for {
            key    <- uuid
            _      <- metaSet(key, 1L)
            result <- metaGet[Long](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(1L, Map.empty)))
        },
        test("boolean") {
          for {
            key    <- uuid
            _      <- metaSet(key, true)
            result <- metaGet[Boolean](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(true, Map.empty)))
        },
        test("float") {
          for {
            key    <- uuid
            _      <- metaSet(key, 1.0f)
            result <- metaGet[Float](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(1.0f, Map.empty)))
        },
        test("double") {
          for {
            key    <- uuid
            _      <- metaSet(key, 1.0d)
            result <- metaGet[Double](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(1.0d, Map.empty)))
        },
        test("option some") {
          for {
            key    <- uuid
            _      <- metaSet(key, Option("value"))
            result <- metaGet[Option[String]](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(Some("value"), Map.empty)))
        },
        test("option none") {
          for {
            key    <- uuid
            _      <- metaSet(key, Option.empty[String])
            result <- metaGet[Option[String]](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(None, Map.empty)))
        },
        test("list") {
          for {
            key    <- uuid
            _      <- metaSet(key, List("value1", "value2"))
            result <- metaGet[List[String]](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(List("value1", "value2"), Map.empty)))
        },
        test("set") {
          for {
            key    <- uuid
            _      <- metaSet(key, Set("value1", "value2"))
            result <- metaGet[Set[String]](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(Set("value1", "value2"), Map.empty)))
        },
        test("map") {
          for {
            key    <- uuid
            _      <- metaSet(key, Map("key1" -> "value1", "key2" -> "value2"))
            result <- metaGet[Map[String, String]](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(Map("key1" -> "value1", "key2" -> "value2"), Map.empty)))
        },
        test("tuple2") {
          for {
            key    <- uuid
            _      <- metaSet(key, ("value1", "value2"))
            result <- metaGet[(String, String)](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue(("value1", "value2"), Map.empty)))
        }
      ),
      suite("set, get and delete with flags")(
        test("add with client flags success") {
          for {
            key    <- uuid
            _      <- metaSet(key, "value", MetaSetFlags.ModeAdd, MetaSetFlags.SetClientFlags(1))
            result <- metaGet[String](key, MetaGetFlags.ReturnClientFlags)
          } yield assert(result)(equalTo(MetaGetResultHeadersOnly(Map('f' -> "1")))) &&
            assert(result.getClientFlags)(isSome(equalTo(1)))
        },
        test("replace with client flags failure") {
          for {
            key    <- uuid
            result <- metaSet(key, "value", MetaSetFlags.ModeReplace, MetaSetFlags.SetClientFlags(1))
          } yield assert(result)(equalTo(MetaSetResultNotStored(Map.empty)))
        },
        test("append") {
          for {
            key    <- uuid
            _      <- metaSet(key, "value")
            _      <- metaSet(key, "---", MetaSetFlags.ModeAppend)
            result <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue("value---", Map.empty)))
        },
        test("prepend") {
          for {
            key    <- uuid
            _      <- metaSet(key, "value")
            _      <- metaSet(key, "---", MetaSetFlags.ModePrepend)
            result <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue("---value", Map.empty)))
        },
        test("compare and swap success") {
          for {
            key       <- uuid
            setResult <- metaSet(key, "value", MetaSetFlags.ReturnItemCasUnique)
            casUnique  = setResult.getCasUnique.get
            result    <- metaSet(key, "value2", MetaSetFlags.CompareCasToken(casUnique))
          } yield assert(result)(equalTo(MetaSetResultStored(Map.empty)))
        },
        test("compare and swap failure") {
          for {
            key       <- uuid
            setResult <- metaSet(key, "value", MetaSetFlags.ReturnItemCasUnique)
            _         <- metaSet(key, "value2", MetaSetFlags.ReturnItemCasUnique)
            casUnique  = setResult.getCasUnique.get
            result    <- metaSet(key, "value3", MetaSetFlags.CompareCasToken(casUnique))
          } yield assert(result)(equalTo(MetaSetResultExists(Map.empty)))
        },
        test("get and touch") {
          for {
            key <- uuid
            _   <- metaSet(key, "value")
            _   <- metaGet[String](key, MetaGetFlags.ReturnItemValue, MetaGetFlags.UpdateRemainingTTL(1))
            delayed <- metaGet[String](key, MetaGetFlags.ReturnItemValue, MetaGetFlags.UpdateRemainingTTL(1))
                         .delay(2.seconds)
                         .fork
            result <- TestClock.adjust(2.seconds) *> delayed.join
          } yield assert(result)(equalTo(MetaGetResultNotFound()))
        },
        test("delete") {
          for {
            key    <- uuid
            _      <- metaSet(key, "value")
            _      <- metaDelete(key)
            result <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultNotFound()))
        },
        test("delete invalidate") {
          for {
            key    <- uuid
            _      <- metaSet(key, "value")
            result <- metaDelete(key, MetaDeleteFlags.Invalidate)
          } yield assert(result)(equalTo(MetaDeleteResultDeleted(Map.empty)))
        },
        test("delete invalidate and win re-cache") {
          for {
            key    <- uuid
            _      <- metaSet(key, "value")
            _      <- metaDelete(key, MetaDeleteFlags.Invalidate)
            first  <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
            second <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
            _      <- metaSet(key, "value2")
            third  <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
          } yield assert(first)(equalTo(MetaGetResultValue("value", Map('X' -> "", 'W' -> "")))) &&
            assert(first.getItemIsStale)(isTrue) && assert(first.getClientWonTheRecacheFlag)(isTrue) &&
            assert(second)(equalTo(MetaGetResultValue("value", Map('X' -> "", 'Z' -> "")))) &&
            assert(second.getItemIsStale)(isTrue) && assert(second.getClientLostTheRecacheFlag)(isTrue) &&
            assert(third)(equalTo(MetaGetResultValue("value2", Map.empty)))
        },
        test("invalidate if cas is older") {
          for {
            key       <- uuid
            _         <- metaSet(key, "value")
            getResult <- metaGet[String](key, MetaGetFlags.ReturnItemCasUnique)
            casUnique  = getResult.getCasUnique.get
            _         <- metaSet(key, "value2")
            _ <- metaSet(
                   key,
                   "value3",
                   MetaSetFlags.CompareCasToken(casUnique),
                   MetaSetFlags.InvalidateIfCasIsOlder
                 )
            result <- metaGet[String](key, MetaGetFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaGetResultValue("value3", Map('X' -> "", 'W' -> "")))) &&
            assert(result.getItemIsStale)(isTrue) && assert(result.getClientWonTheRecacheFlag)(isTrue)
        },
        test("base64 key") {
          for {
            key       <- uuid
            base64     = Base64.getEncoder.encodeToString(key.getBytes(StandardCharsets.UTF_8))
            setResult <- metaSet(base64, "value", MetaSetFlags.InterpretKeyAsBase64)
            getResult <- metaGet[String](
                           base64,
                           MetaGetFlags.ReturnItemValue,
                           MetaGetFlags.InterpretKeyAsBase64,
                           MetaGetFlags.ReturnKeyAsToken
                         )
            delResult <- metaDelete(base64, MetaDeleteFlags.InterpretKeyAsBase64)
          } yield assert(setResult)(equalTo(MetaSetResultStored(Map.empty))) &&
            assert(getResult)(equalTo(MetaGetResultValue("value", Map('k' -> base64, 'b' -> "")))) &&
            assert(getResult.getKey)(isSome(equalTo(base64))) && assert(getResult.isKeyBase64Encoded)(isTrue) &&
            assert(delResult)(equalTo(MetaDeleteResultDeleted(Map.empty)))
        }
      ),
      suite("arithmetics")(
        test("incr") {
          for {
            key    <- uuid
            _      <- metaSet(key, "1")
            result <- metaArithmetic(key, MetaArithmeticFlags.ReturnItemValue)
          } yield assert(result)(equalTo(MetaArithmeticResultExists(Map.empty, Some(2L))))
        },
        test("incr with initial value") {
          for {
            key <- uuid
            result <- metaArithmetic(
                        key,
                        MetaArithmeticFlags.ReturnItemValue,
                        MetaArithmeticFlags.CreateItemOnMiss(60 * 60),
                        MetaArithmeticFlags.InitialValue(1)
                      )
          } yield assert(result)(equalTo(MetaArithmeticResultExists(Map.empty, Some(1L))))
        },
        test("incr with initial value and expiration") {
          for {
            key <- uuid
            result1 <- metaArithmetic(
                         key,
                         MetaArithmeticFlags.ReturnItemValue,
                         MetaArithmeticFlags.CreateItemOnMiss(1),
                         MetaArithmeticFlags.InitialValue(1)
                       )
            sleeper <- Clock.sleep(2.seconds).fork
            _       <- TestClock.adjust(2.seconds) *> sleeper.join
            result2 <- metaArithmetic(
                         key,
                         MetaArithmeticFlags.ReturnItemValue,
                         MetaArithmeticFlags.CreateItemOnMiss(1),
                         MetaArithmeticFlags.InitialValue(1)
                       )
          } yield assert(result1)(equalTo(MetaArithmeticResultExists(Map.empty, Some(1L)))) &&
            assert(result2)(equalTo(MetaArithmeticResultExists(Map.empty, Some(1L))))
        }
      ),
      suite("other commands")(
        test("meta debug") {
          for {
            key    <- uuid
            _      <- metaSet(key, "value")
            result <- metaDebug(key)
          } yield assert(result.headers)(contains("key" -> key))
        },
        test("meta debug not existing") {
          for {
            key    <- uuid
            result <- metaDebug(key)
          } yield assert(result)(equalTo(MetaDebugResultNotFound))
        }
      )
    )
}
