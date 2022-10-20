package zio.memcached

import zio._
import zio.schema.codec.{Codec, ProtobufCodec}
import zio.test.TestAspect.tag
import zio.test._

import java.time.Instant
import java.util.UUID

trait BaseSpec extends ZIOSpecDefault {
  implicit val codec: Codec = ProtobufCodec

  override def aspects: Chunk[TestAspectAtLeastR[Live]] =
    Chunk.succeed(TestAspect.timeout(10.seconds))

  def instantOf(millis: Long): UIO[Instant] = ZIO.succeed(Instant.now().plusMillis(millis))

  final val genPatternOption: Gen[Any, Option[String]] =
    Gen.option(Gen.constSample(Sample.noShrink("*")))

  final val uuid: UIO[String] =
    ZIO.succeed(UUID.randomUUID().toString)

  final val testExecutorUnsupported: TestAspectPoly =
    tag(BaseSpec.TestExecutorUnsupported)
}

object BaseSpec {
  final val TestExecutorUnsupported = "test executor unsupported"
}
