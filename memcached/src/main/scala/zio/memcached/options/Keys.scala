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

package zio.memcached.options

trait Keys {

  case object AbsTtl {
    private[memcached] def stringify: String = "ABSTTL"
  }

  type AbsTtl = AbsTtl.type

  case object Alpha {
    private[memcached] def stringify: String = "ALPHA"
  }

  type Alpha = Alpha.type

  sealed case class Auth(password: String)

  sealed case class By(pattern: String)

  case object Copy {
    private[memcached] def stringify: String = "COPY"
  }

  type Copy = Copy.type

  sealed case class IdleTime(seconds: Long)

  sealed case class Freq(frequency: String)

  sealed trait MemcachedType extends Product with Serializable { self =>
    private[memcached] final def stringify: String =
      self match {
        case MemcachedType.String    => "string"
        case MemcachedType.List      => "list"
        case MemcachedType.Set       => "set"
        case MemcachedType.SortedSet => "zset"
        case MemcachedType.Hash      => "hash"
        case MemcachedType.Stream    => "stream"
      }
  }

  object MemcachedType {
    case object String    extends MemcachedType
    case object List      extends MemcachedType
    case object Set       extends MemcachedType
    case object SortedSet extends MemcachedType
    case object Hash      extends MemcachedType
    case object Stream    extends MemcachedType
  }

  case object Replace {
    private[memcached] def stringify: String = "REPLACE"
  }

  type Replace = Replace.type
}
