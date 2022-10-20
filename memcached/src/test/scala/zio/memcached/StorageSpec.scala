package zio.memcached

import zio.test.Assertion.{exists => _, _}
import zio.test._

trait StorageSpec extends BaseSpec {
  def stringsSuite: Spec[Memcached, MemcachedError] =
    suite("strings")(
      suite("get")(
        test("non-emtpy string") {
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
        }
      )
    )
}
