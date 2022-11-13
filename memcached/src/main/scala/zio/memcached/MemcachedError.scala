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

import java.io.IOException
import scala.util.control.NoStackTrace

sealed trait MemcachedError extends NoStackTrace

object MemcachedError {
  final case class ProtocolError(message: String) extends MemcachedError {
    override def toString: String = s"ProtocolError: $message"
  }
  final case class CodecError(message: String) extends MemcachedError {
    override def toString: String = s"CodecError: $message"
  }
  final case class NotConnected(message: String)   extends MemcachedError
  final case class IOError(exception: IOException) extends MemcachedError
  case object InvalidKey                           extends MemcachedError
  case object Interrupted                          extends MemcachedError
  case object ConnectionClosed                     extends MemcachedError
}
