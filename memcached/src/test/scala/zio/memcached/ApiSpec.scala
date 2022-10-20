package zio.memcached

import zio._
import zio.test.TestAspect._

object ApiSpec extends StorageSpec {

  def spec =
    suite("Memcached commands")(
      suite("Live Executor")(
        stringsSuite
      ).provideLayerShared(LiveLayer) @@ sequential @@ withLiveEnvironment
      // suite("Test Executor")(
      //   listSuite,
      //   stringsSuite
      // ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(BaseSpec.TestExecutorUnsupported))
      //   .get
      //   .provideCustomLayer(TestLayer)
    )

  private val LiveLayer =
    ZLayer.make[Memcached](MemcachedExecutor.local, MemcachedLive.layer, ZLayer.succeed(codec))

  // private val TestLayer =
  //   ZLayer.make[Memcached](MemcachedExecutor.test, MemcachedLive.layer, ZLayer.succeed(codec))
}
