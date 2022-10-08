package zio.memcached

import zio.test.Assertion._
import zio.test._

trait HyperLogLogSpec extends BaseSpec {
  def hyperLogLogSuite: Spec[Memcached, MemcachedError] =
    suite("hyperloglog")(
      suite("add elements")(
        test("pfAdd elements to key") {
          for {
            key <- uuid
            add <- pfAdd(key, "one", "two", "three")
          } yield assert(add)(equalTo(true))
        },
        test("pfAdd nothing to key when new elements not unique") {
          for {
            key  <- uuid
            add1 <- pfAdd(key, "one", "two", "three")
            add2 <- pfAdd(key, "one", "two", "three")
          } yield assert(add1)(equalTo(true)) && assert(add2)(equalTo(false))
        },
        test("pfAdd error when not hyperloglog") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            add   <- pfAdd(key, "one", "two", "three").either
          } yield assert(add)(isLeft(isSubtype[MemcachedError.WrongType](anything)))
        }
      ),
      suite("count elements")(
        test("pfCount zero at undefined key") {
          for {
            count <- pfCount("noKey")
          } yield assert(count)(equalTo(0L))
        },
        test("pfCount values at key") {
          for {
            key   <- uuid
            add   <- pfAdd(key, "one", "two", "three")
            count <- pfCount(key)
          } yield assert(add)(equalTo(true)) && assert(count)(equalTo(3L))
        },
        test("pfCount union key with key2") {
          for {
            key   <- uuid
            key2  <- uuid
            add   <- pfAdd(key, "one", "two", "three")
            add2  <- pfAdd(key2, "four", "five", "six")
            count <- pfCount(key, key2)
          } yield assert(add)(equalTo(true)) && assert(add2)(equalTo(true)) && assert(count)(equalTo(6L))
        },
        test("error when not hyperloglog") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            count <- pfCount(key).either
          } yield assert(count)(isLeft)
        }
      ),
      suite("merge")(
        test("pfMerge two hyperloglogs and create destination") {
          for {
            key   <- uuid
            key2  <- uuid
            key3  <- uuid
            _     <- pfAdd(key, "one", "two", "three", "four")
            _     <- pfAdd(key2, "five", "six", "seven")
            _     <- pfMerge(key3, key2, key)
            count <- pfCount(key3)
          } yield assert(count)(equalTo(7L))
        },
        test("pfMerge two hyperloglogs with already existing destination values") {
          for {
            key   <- uuid
            key2  <- uuid
            key3  <- uuid
            _     <- pfAdd(key, "one", "two", "three", "four")
            _     <- pfAdd(key2, "five", "six", "seven")
            _     <- pfAdd(key3, "eight", "nine", "ten")
            _     <- pfMerge(key3, key2, key)
            count <- pfCount(key3)
          } yield assert(count)(equalTo(10L))
        },
        test("pfMerge error when source not hyperloglog") {
          for {
            key   <- uuid
            value <- uuid
            key2  <- uuid
            key3  <- uuid
            _     <- set(key, value, None, None, None)
            _     <- pfAdd(key2, "five", "six", "seven")
            merge <- pfMerge(key3, key2, key).either
          } yield assert(merge)(isLeft)
        }
      )
    )
}
