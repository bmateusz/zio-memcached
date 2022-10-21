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

import zio.memcached.MemcachedConfig.MemcachedNode

final case class MemcachedConfig(nodes: List[MemcachedNode])

object MemcachedConfig {
  lazy val Default: MemcachedConfig = MemcachedConfig("localhost", 11211)

  final case class MemcachedNode(host: String, port: Int)

  object MemcachedNode {
    def apply(host: String, port: Int): MemcachedNode = new MemcachedNode(host, port)
  }

  def apply(host: String, port: Int): MemcachedConfig = new MemcachedConfig(MemcachedNode(host, port) :: Nil)
}
