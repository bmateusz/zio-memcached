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

package zio.memcached.benchmarks

import net.spy.memcached.MemcachedClient
import zio._
import zio.memcached._
import zio.schema.codec.{Codec, ProtobufCodec}

import java.net.InetSocketAddress

trait BenchmarkRuntime {

  import zio.memcached.benchmarks.BenchmarkRuntime._

  final lazy val runtime: Runtime[Memcached] = {
    Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.unsafe.fromLayer(Layer)
    }
  }

  final def execute(query: ZIO[Memcached, MemcachedError, Unit]): Unit =
    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe.run(query).getOrThrowFiberFailure()
    }

  final lazy val SpyMemcached =
    new MemcachedClient(
      new InetSocketAddress(MemcachedHost, MemcachedPort1),
      new InetSocketAddress(MemcachedHost, MemcachedPort2)
    )
}

object BenchmarkRuntime {
  private final val Layer =
    ZLayer.make[Memcached](
      MemcachedExecutor.local,
      ZLayer.succeed[Codec](ProtobufCodec),
      MemcachedLive.layer
    )

  final val MemcachedHost = "127.0.0.1"
  final val MemcachedPort1 = 11211
  final val MemcachedPort2 = 11212
}
