package zio.memcached

import zio.test.Assertion.{exists => _, _}
import zio.test._
import zio._
import zio.memcached.BaseSpec.Person
import zio.memcached.model.UpdateResult
import zio.schema.DeriveSchema.gen

trait StorageSpec extends BaseSpec {
  def storageSuite: Spec[Memcached, MemcachedError] =
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
        test("byte") {
          for {
            key    <- uuid
            _      <- set(key, Byte.MaxValue)
            result <- get[Byte](key)
          } yield assert(result)(isSome(equalTo(127: Byte)))
        },
        test("short") {
          for {
            key    <- uuid
            _      <- set(key, Short.MinValue)
            result <- get[Short](key)
          } yield assert(result)(isSome(equalTo(-32768: Short)))
        },
        test("int") {
          for {
            key    <- uuid
            _      <- set(key, 1)
            result <- get[Int](key)
          } yield assert(result)(isSome(equalTo(1)))
        },
        test("char max") {
          for {
            key    <- uuid
            _      <- set(key, Char.MaxValue)
            result <- get[Char](key)
          } yield assert(result)(isSome(equalTo(65535: Char)))
        },
        test("char min") {
          for {
            key    <- uuid
            _      <- set(key, '\u0000')
            result <- get[Char](key)
          } yield assert(result)(isSome(equalTo('\u0000')))
        },
        test("long") {
          for {
            key    <- uuid
            _      <- set(key, Long.MaxValue)
            result <- get[Long](key)
          } yield assert(result)(isSome(equalTo(9223372036854775807L: Long)))
        },
        test("boolean") {
          for {
            key    <- uuid
            _      <- set(key, true)
            result <- get[Boolean](key)
          } yield assert(result)(isSome(equalTo(true)))
        },
        test("float") {
          for {
            key    <- uuid
            _      <- set(key, 1.0f)
            result <- get[Float](key)
          } yield assert(result)(isSome(equalTo(1.0f)))
        },
        test("double") {
          for {
            key    <- uuid
            _      <- set(key, 1.0)
            result <- get[Double](key)
          } yield assert(result)(isSome(equalTo(1.0)))
        },
        test("option some") {
          for {
            key    <- uuid
            _      <- set(key, Option[String]("a"))
            result <- get[Option[String]](key)
          } yield assert(result)(isSome(isSome(equalTo("a"))))
        },
        test("option none") {
          for {
            key    <- uuid
            _      <- set(key, Option[String](null))
            result <- get[Option[String]](key)
          } yield assert(result)(isSome(isNone))
        },
        test("list of strings") {
          for {
            key    <- uuid
            _      <- set(key, List[String]("a", "b", "c"))
            result <- get[List[String]](key)
          } yield assert(result)(isSome(equalTo(List[String]("a", "b", "c"))))
        },
        test("set") {
          for {
            key    <- uuid
            _      <- set(key, Set[String]("a", "b", "c"))
            result <- get[Set[String]](key)
          } yield assert(result)(isSome(equalTo(Set[String]("a", "b", "c"))))
        },
        test("map") {
          for {
            key    <- uuid
            _      <- set(key, Map[String, String]("a" -> "b", "c" -> "d"))
            result <- get[Map[String, String]](key)
          } yield assert(result)(isSome(equalTo(Map[String, String]("a" -> "b", "c" -> "d"))))
        },
        test("tuple2") {
          for {
            key    <- uuid
            _      <- set(key, ("a", "b"))
            result <- get[(String, String)](key)
          } yield assert(result)(isSome(equalTo(("a", "b"))))
        },
        test("byte array chunk") {
          for {
            key    <- uuid
            _      <- set(key, Chunk[Byte](1, 2, 3))
            result <- get[Chunk[Byte]](key)
          } yield assert(result)(isSome(equalTo(Chunk[Byte](1, 2, 3))))
        },
        test("case class") {
          for {
            key    <- uuid
            _      <- set(key, Person("name", 1))
            result <- get[Person](key)
          } yield assert(result)(isSome(equalTo(Person("name", 1))))
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
            _      <- set(key, "value", Some(1.second))
            result <- get[String](key)
          } yield assert(result)(isSome(equalTo("value")))
        },
        test("set ttl and expire") {
          for {
            key    <- uuid
            _      <- set(key, "value", Some(1.second))
            _      <- ZIO.sleep(2.seconds)
            result <- get[String](key)
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
            key    <- uuid
            _      <- set(key, "value", None)
            _      <- touch(key, 1.seconds)
            _      <- ZIO.sleep(2.seconds)
            result <- get[String](key)
          } yield assert(result)(isNone)
        },
        test("set ttl with get and touch") {
          for {
            key       <- uuid
            _         <- set(key, "value", None)
            gatResult <- getAndTouch[String](key, 1.seconds)
            _         <- ZIO.sleep(2.seconds)
            getResult <- get[String](key)
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
      suite("compare and set")(
        test("cas updated") {
          for {
            key             <- uuid
            _               <- set(key, "value")
            optValueWithCas <- getWithCas[String](key)
            casRes          <- compareAndSet(key, "value2", optValueWithCas.get._1)
            getRes          <- get[String](key)
          } yield assert(casRes)(equalTo(UpdateResult.Updated)) && assert(getRes)(isSome(equalTo("value2")))
        },
        test("cas exists (not updated)") {
          for {
            key             <- uuid
            _               <- set(key, "value")
            optValueWithCas <- getAndTouchWithCas[String](key, 1.day)
            _               <- set(key, "value2")
            casRes          <- compareAndSet(key, "value3", optValueWithCas.get._1)
            getRes          <- get[String](key)
          } yield assert(casRes)(equalTo(UpdateResult.Exists)) && assert(getRes)(isSome(equalTo("value2")))
        },
        test("cas not exists") {
          for {
            key             <- uuid
            _               <- set(key, "value")
            optValueWithCas <- getWithCas[String](key)
            _               <- delete(key)
            casRes          <- compareAndSet(key, "value2", optValueWithCas.get._1)
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
          } yield assert(incRes)(equalTo(2L)) && assert(getRes)(isSome(equalTo("2")))
        },
        test("decrement number in string representation") {
          for {
            key    <- uuid
            _      <- set(key, "6")
            incRes <- increment(key, -1)
            getRes <- get[String](key)
          } yield assert(incRes)(equalTo(5L)) && assert(getRes)(isSome(equalTo("5")))
        },
        test("decrement number underflow in string representation") {
          for {
            key    <- uuid
            _      <- set(key, "5")
            incRes <- increment(key, -6)
            getRes <- get[String](key)
          } yield assert(incRes)(equalTo(0L)) && assert(getRes)(isSome(equalTo("0")))
        } /*
        test("increment number overflow in string representation") {
          for {
            key    <- uuid
            _      <- set(key, "9223372036854775807")
            incRes <- increment(key, 1)
            getRes <- get[String](key)
          } yield assert(incRes)(equalTo(0L)) && assert(getRes)(isSome(equalTo("0")))
        },
        test("increment number in binary representation (results in error)") {
          for {
            key    <- uuid
            _      <- set(key, 1L)
            incRes <- increment(key, 1)
          } yield assert(incRes)(equalTo(2L))
        },
        test("increment non existing key") {
          for {
            key    <- uuid
            incRes <- increment(key, 1L)
          } yield assert(incRes)(equalTo(1L))
        },*/
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
      )
    )
}
