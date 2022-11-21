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
      ).provideLayerShared(TestLayer)
    ) @@ parallelN(2)

  private val LiveLayer =
    ZLayer.make[Memcached](MemcachedExecutor.local, MemcachedLive.layer, ZLayer.succeed(codec))

  private val TestLayer =
    ZLayer.make[Memcached](MemcachedExecutor.test, MemcachedLive.layer, ZLayer.succeed(codec))
}
