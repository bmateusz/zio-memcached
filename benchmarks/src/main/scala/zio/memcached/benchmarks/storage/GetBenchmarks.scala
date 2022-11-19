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

package zio.memcached.benchmarks.storage

import org.openjdk.jmh.annotations._
import zio.memcached._
import zio.memcached.benchmarks._
import zio.{Scope => _, _}

import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 1)
@Warmup(iterations = 1)
@Fork(2)
class GetBenchmarks extends BenchmarkRuntime {

  @Param(Array("500"))
  var count: Int = _

  private var items: List[String] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    execute(ZIO.foreachDiscard(items)(i => set(i, i)))
  }

  @Benchmark
  def spyMemcached(): Unit =
    execute(ZIO.foreachDiscard(items)(i => ZIO.attempt(SpyMemcached.get(i)).ignore))

  @Benchmark
  def zio(): Unit = execute(ZIO.foreachDiscard(items)(get[String](_)))

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    val _ = SpyMemcached.shutdown(0, TimeUnit.SECONDS)
  }
}
