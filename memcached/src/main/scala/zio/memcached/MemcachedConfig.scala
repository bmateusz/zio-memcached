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

import zio.Schedule.WithState
import zio.memcached.MemcachedConfig._
import zio.{Schedule, durationInt}

final case class MemcachedConfig(
  nodes: List[MemcachedNode],
  retrySchedule: Schedule[Any, Any, Any] = DefaultRetrySchedule,
  hashAlgorithm: String => Int = DefaultHashAlgorithm,
  messageQueueSize: Int = DefaultMessageQueueSize
)

object MemcachedConfig {

  final case class MemcachedNode(host: String, port: Int)

  lazy val Default: MemcachedConfig = MemcachedConfig(
    MemcachedNode("0.0.0.0", 11211) :: MemcachedNode("0.0.0.0", 11212) :: Nil
  )

  lazy val DefaultRetrySchedule: WithState[Long, Any, Any, Long] = Schedule.spaced(5.second)

  lazy val DefaultHashAlgorithm: String => Int = (key: String) => key.hashCode

  lazy val DefaultMessageQueueSize: Int = 16

}
