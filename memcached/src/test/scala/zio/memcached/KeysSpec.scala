package zio.memcached

import zio._
import zio.memcached.MemcachedError.ProtocolError
import zio.schema.codec.{Codec, ProtobufCodec}
import zio.test.Assertion.{exists => _, _}
import zio.test.TestAspect.{restore => _, _}
import zio.test._

trait KeysSpec extends BaseSpec {
  def keysSuite: Spec[Memcached with TestEnvironment, MemcachedError] = {
    suite("keys")(
      test("set followed by get") {
        for {
          key   <- uuid
          value <- uuid
          _     <- set(key, value)
          v     <- get(key).returning[String]
        } yield assert(v)(isSome(equalTo(value)))
      },
      test("get non-existing key") {
        for {
          key <- uuid
          v   <- get(key).returning[String]
        } yield assert(v)(isNone)
      },
      test("handles wrong types") {
        for {
          key <- uuid
          _   <- sAdd(key, "1", "2", "3")
          v   <- get(key).returning[String].either
        } yield assert(v)(isLeft)
      },
      test("check whether or not key exists") {
        for {
          key   <- uuid
          value <- uuid
          _     <- set(key, value)
          e1    <- exists(key)
          e2    <- exists("unknown")
        } yield assert(e1)(equalTo(1L)) && assert(e2)(equalTo(0L))
      },
      test("delete existing key") {
        for {
          key     <- uuid
          value   <- uuid
          _       <- set(key, value)
          deleted <- del(key)
        } yield assert(deleted)(equalTo(1L))
      },
      test("find all keys matching pattern") {
        for {
          value    <- uuid
          key1      = "custom_key_1"
          key2      = "custom_key_2"
          _        <- set(key1, value)
          _        <- set(key2, value)
          response <- keys("*custom*").returning[String]
        } yield assert(response)(hasSameElements(Chunk(key1, key2)))
      },
      test("unlink existing key") {
        for {
          key     <- uuid
          value   <- uuid
          _       <- set(key, value)
          removed <- unlink(key)
        } yield assert(removed)(equalTo(1L))
      },
      test("touch two existing keys") {
        for {
          key1    <- uuid
          value1  <- uuid
          _       <- set(key1, value1)
          key2    <- uuid
          value2  <- uuid
          _       <- set(key2, value2)
          touched <- touch(key1, key2)
        } yield assert(touched)(equalTo(2L))
      },
      test("scan entries with match, count and type options")(
        check(genPatternOption, genCountOption, genStringMemcachedTypeOption) { (pattern, count, memcachedType) =>
          for {
            key             <- uuid
            value           <- uuid
            _               <- set(key, value)
            scan            <- scan(0L, pattern, count, memcachedType).returning[String]
            (next, elements) = scan
          } yield assert(next)(isGreaterThanEqualTo(0L)) && assert(elements)(isNonEmpty)
        }
      ) @@ flaky,
      test("fetch random key") {
        for {
          key       <- uuid
          value     <- uuid
          _         <- set(key, value)
          allKeys   <- keys("*").returning[String]
          randomKey <- randomKey.returning[String]
        } yield assert(allKeys)(contains(randomKey.get))
      } @@ ignore,
      test("dump followed by restore") {
        for {
          key      <- uuid
          value    <- uuid
          _        <- set(key, value)
          dumped   <- dump(key)
          _        <- del(key)
          restore  <- restore(key, 0L, dumped).either
          restored <- get(key).returning[String]
        } yield assert(restore)(isRight) && assert(restored)(isSome(equalTo(value)))
      } @@ ignore,
      suite("migrate")(
        test("migrate key to another memcached server (copy and replace)") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            response <- migrate(
                          "memcached2",
                          6379,
                          key,
                          0L,
                          KeysSpec.MigrateTimeout,
                          copy = Option(Copy),
                          replace = Option(Replace),
                          keys = None
                        )
            originGet <- get(key).returning[String]
            destGet   <- get(key).returning[String].provideLayer(KeysSpec.SecondExecutor)
          } yield assert(response)(equalTo("OK")) &&
            assert(originGet)(isSome(equalTo(value))) &&
            assert(destGet)(isSome(equalTo(value)))
        },
        test("migrate key to another memcached server (move and replace)") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            response <-
              migrate(
                "memcached2",
                6379,
                key,
                0L,
                KeysSpec.MigrateTimeout,
                copy = None,
                replace = Option(Replace),
                keys = None
              )
            originGet <- get(key).returning[String]
            destGet   <- get(key).returning[String].provideLayer(KeysSpec.SecondExecutor)
          } yield assert(response)(equalTo("OK")) &&
            assert(originGet)(isNone) &&
            assert(destGet)(isSome(equalTo(value)))
        },
        test("migrate key to another memcached server (move and no replace, should fail when key exists)") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            _     <- set(key, value).provideLayer(KeysSpec.SecondExecutor) // also add to second Memcached
            response <-
              migrate("memcached2", 6379, key, 0L, KeysSpec.MigrateTimeout, copy = None, replace = None, keys = None).either
          } yield assert(response)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ) @@ testExecutorUnsupported,
      suite("ttl")(
        test("check ttl for existing key") {
          for {
            key   <- uuid
            value <- uuid
            _     <- pSetEx(key, 1000.millis, value)
            ttl   <- ttl(key).either
          } yield assert(ttl)(isRight)
        } @@ eventually,
        test("check ttl for non-existing key") {
          for {
            key <- uuid
            ttl <- ttl(key).either
          } yield assert(ttl)(isLeft)
        },
        test("check pTtl for existing key") {
          for {
            key   <- uuid
            value <- uuid
            _     <- pSetEx(key, 1000.millis, value)
            pTtl  <- pTtl(key).either
          } yield assert(pTtl)(isRight)
        } @@ eventually,
        test("check pTtl for non-existing key") {
          for {
            key  <- uuid
            pTtl <- pTtl(key).either
          } yield assert(pTtl)(isLeft)
        }
      ),
      suite("expiration")(
        test("set key expiration with pExpire command") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            exp       <- pExpire(key, 2000.millis)
            response1 <- exists(key)
            fiber     <- ZIO.sleep(2050.millis).fork <* TestClock.adjust(2050.millis)
            _         <- fiber.join
            response2 <- exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(equalTo(1L)) && assert(response2)(equalTo(0L))
        },
        test("set key expiration with pExpireAt command") {
          for {
            key       <- uuid
            value     <- uuid
            expiresAt <- Clock.instant.map(_.plusMillis(2000.millis.toMillis))
            _         <- set(key, value)
            exp       <- pExpireAt(key, expiresAt)
            response1 <- exists(key)
            fiber     <- ZIO.sleep(2050.millis).fork <* TestClock.adjust(2050.millis)
            _         <- fiber.join
            response2 <- exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(equalTo(1L)) && assert(response2)(equalTo(0L))
        },
        test("expire followed by persist") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            exp       <- expire(key, 10000.millis)
            persisted <- persist(key)
          } yield assert(exp)(isTrue) && assert(persisted)(isTrue)
        },
        test("set key expiration with expireAt command") {
          for {
            key       <- uuid
            value     <- uuid
            expiresAt <- Clock.instant.map(_.plusMillis(2000.millis.toMillis))
            _         <- set(key, value)
            exp       <- expireAt(key, expiresAt)
            response1 <- exists(key)
            fiber     <- ZIO.sleep(2050.millis).fork <* TestClock.adjust(2050.millis)
            _         <- fiber.join
            response2 <- exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(equalTo(1L)) && assert(response2)(equalTo(0L))
        }
      ),
      suite("renaming")(
        test("rename existing key") {
          for {
            key     <- uuid
            newKey  <- uuid
            value   <- uuid
            _       <- set(key, value)
            renamed <- rename(key, newKey).either
            v       <- get(newKey).returning[String]
          } yield assert(renamed)(isRight) && assert(v)(isSome(equalTo(value)))
        },
        test("try to rename non-existing key") {
          for {
            key     <- uuid
            newKey  <- uuid
            renamed <- rename(key, newKey).either
          } yield assert(renamed)(isLeft)
        },
        test("rename key to newkey if newkey does not yet exist") {
          for {
            key     <- uuid
            newKey  <- uuid
            value   <- uuid
            _       <- set(key, value)
            renamed <- renameNx(key, newKey)
            v       <- get(newKey).returning[String]
          } yield assert(renamed)(isTrue) && assert(v)(isSome(equalTo(value)))
        },
        test("try to rename non-existing key with renameNx command") {
          for {
            key     <- uuid
            newKey  <- uuid
            renamed <- renameNx(key, newKey).either
          } yield assert(renamed)(isLeft)
        }
      ),
      suite("types")(
        test("string type") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value)
            string <- typeOf(key)
          } yield assert(string)(equalTo(MemcachedType.String))
        },
        test("list type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- lPush(key, value)
            list  <- typeOf(key)
          } yield assert(list)(equalTo(MemcachedType.List))
        },
        test("set type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- sAdd(key, value)
            set   <- typeOf(key)
          } yield assert(set)(equalTo(MemcachedType.Set))
        },
        test("sorted set type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- zAdd(key)(MemberScore(1d, value))
            zset  <- typeOf(key)
          } yield assert(zset)(equalTo(MemcachedType.SortedSet))
        },
        test("hash type") {
          for {
            key   <- uuid
            field <- uuid
            value <- uuid
            _     <- hSet(key, (field, value))
            hash  <- typeOf(key)
          } yield assert(hash)(equalTo(MemcachedType.Hash))
        },
        test("stream type") {
          for {
            key    <- uuid
            field  <- uuid
            value  <- uuid
            _      <- xAdd(key, "*", (field, value)).returning[String]
            stream <- typeOf(key)
          } yield assert(stream)(equalTo(MemcachedType.Stream))
        } @@ testExecutorUnsupported
      ),
      suite("sort")(
        test("list of numbers") {
          for {
            key    <- uuid
            _      <- lPush(key, "1", "0", "2")
            sorted <- sort(key).returning[String]
          } yield assert(sorted)(equalTo(Chunk("0", "1", "2")))
        },
        test("list of strings") {
          for {
            key    <- uuid
            _      <- lPush(key, "z", "a", "c")
            sorted <- sort(key, alpha = Some(Alpha)).returning[String]
          } yield assert(sorted)(equalTo(Chunk("a", "c", "z")))
        },
        test("list of numbers, limited") {
          for {
            key    <- uuid
            _      <- lPush(key, "1", "0", "2")
            sorted <- sort(key, limit = Some(Limit(1, 1))).returning[String]
          } yield assert(sorted)(equalTo(Chunk("1")))
        },
        test("descending sort") {
          for {
            key    <- uuid
            _      <- lPush(key, "1", "0", "2")
            sorted <- sort(key, order = Order.Descending).returning[String]
          } yield assert(sorted)(equalTo(Chunk("2", "1", "0")))
        },
        test("by the value referenced by a key-value pair") {
          for {
            key    <- uuid
            a      <- uuid
            b      <- uuid
            _      <- lPush(key, b, a)
            prefix <- uuid
            _      <- set(s"${prefix}_$a", "A")
            _      <- set(s"${prefix}_$b", "B")
            sorted <- sort(key, by = Some(s"${prefix}_*"), alpha = Some(Alpha)).returning[String]
          } yield assert(sorted)(equalTo(Chunk(a, b)))
        },
        test("getting the value referenced by a key-value pair") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- lPush(key, value)
            prefix <- uuid
            _      <- set(s"${prefix}_$value", "A")
            sorted <- sort(key, get = Some(s"${prefix}_*" -> List.empty), alpha = Some(Alpha)).returning[String]
          } yield assert(sorted)(equalTo(Chunk("A")))
        },
        test("getting multiple values referenced by a key-value pair") {
          for {
            key     <- uuid
            value1  <- uuid
            value2  <- uuid
            _       <- lPush(key, value1, value2)
            prefix  <- uuid
            _       <- set(s"${prefix}_$value1", "A1")
            _       <- set(s"${prefix}_$value2", "A2")
            prefix2 <- uuid
            _       <- set(s"${prefix2}_$value1", "B1")
            _       <- set(s"${prefix2}_$value2", "B2")
            sorted <- sort(key, get = Some((s"${prefix}_*", List(s"${prefix2}_*"))), alpha = Some(Alpha))
                        .returning[String]
          } yield assert(sorted)(equalTo(Chunk("A1", "B1", "A2", "B2")))
        } @@ flaky,
        test("sort and store result") {
          for {
            key       <- uuid
            resultKey <- uuid
            _         <- lPush(key, "1", "0", "2")
            count     <- sortStore(key, Store(resultKey))
            sorted    <- lRange(resultKey, 0 to 2).returning[String]
          } yield assert(sorted)(equalTo(Chunk("0", "1", "2"))) && assert(count)(equalTo(3L))
        }
      )
    )
  }
}

object KeysSpec {
  final val MigrateTimeout: Duration = 5.seconds

  final val SecondExecutor: Layer[MemcachedError.IOError, Memcached] =
    ZLayer
      .make[Memcached](
        ZLayer.succeed(MemcachedConfig("localhost", 6380)),
        MemcachedExecutor.layer,
        ZLayer.succeed[Codec](ProtobufCodec),
        MemcachedLive.layer
      )
      .fresh
}
