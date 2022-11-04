package zio.memcached

import zio._
import zio.test.Assertion._
import zio.test._

object ByteStreamSpec extends BaseSpec {
  def spec: Spec[Any, Throwable] =
    suite("Byte stream")(
      test("can write and read") {
        for {
          streams <- ZIO.service[Chunk[ByteStream]]
          stream   = streams.head
          data     = Chunk.fromArray("mn\r\n".getBytes())
          _       <- stream.write(data)
          res     <- stream.read.take(1).runCollect
        } yield assert(res)(equalTo(Chunk.single(Chunk.fromArray("MN\r\n".getBytes()))))
      }
    ).provideLayer(ByteStream.default)
}
