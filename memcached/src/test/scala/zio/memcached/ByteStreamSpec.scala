package zio.memcached

import zio._
import zio.test.Assertion._
import zio.test._

import java.nio.charset.StandardCharsets

object ByteStreamSpec extends BaseSpec {
  def spec: Spec[Any, Throwable] =
    suite("Byte stream")(
      test("can write and read") {
        for {
          streams <- ZIO.service[Chunk[ByteStream]]
          stream   = streams.head
          data     = Chunk.fromArray("mn\r\n".getBytes(StandardCharsets.US_ASCII))
          _       <- stream.write(data)
          res     <- stream.read.take(4).runCollect
        } yield assert(res)(equalTo(Chunk.fromArray("MN\r\n".getBytes(StandardCharsets.US_ASCII))))
      }
    ).provideLayer(ByteStream.default)
}
