package zio.memcached

import zio._
import zio.test.TestAnnotation
import zio.test.TestAspect._

object ApiSpec extends StorageSpec with MetaSpec {

  def spec =
    suite("Memcached commands")(
      suite("Live Executor")(
        storageSuite,
        metaSuite
      ).provideLayerShared(LiveLayer) @@ withLiveEnvironment,
      suite("Test Executor")(
        storageSuite,
        metaSuite
      ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(BaseSpec.TestExecutorUnsupported))
        .get
        .provideLayerShared(TestLayer)
    )

  private val LiveLayer =
    ZLayer.make[Memcached](MemcachedExecutor.local, MemcachedLive.layer, ZLayer.succeed(codec))

  private val TestLayer =
    ZLayer.make[Memcached](MemcachedExecutor.test, MemcachedLive.layer, ZLayer.succeed(codec))
}
