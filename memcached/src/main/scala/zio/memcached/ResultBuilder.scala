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

import zio.ZIO
import zio.memcached.ResultBuilder.NeedsReturnType
import zio.schema.Schema

sealed trait ResultBuilder {

  final def map(f: Nothing => Any)(implicit nrt: NeedsReturnType): ZIO[Any, Nothing, Nothing] = ???

  final def flatMap(f: Nothing => Any)(implicit nrt: NeedsReturnType): ZIO[Any, Nothing, Nothing] = ???
}

object ResultBuilder {

  @annotation.implicitNotFound("Use `returning[A]` to specify method's return type")
  final abstract class NeedsReturnType

  trait ResultBuilder1[+F[_]] extends ResultBuilder {
    def returning[R: Schema]: ZIO[Memcached, MemcachedError, F[R]]
  }

  trait ResultBuilder2[+F[_, _]] extends ResultBuilder {
    def returning[R1: Schema, R2: Schema]: ZIO[Memcached, MemcachedError, F[R1, R2]]
  }

  trait ResultBuilder3[+F[_, _, _]] extends ResultBuilder {
    def returning[R1: Schema, R2: Schema, R3: Schema]: ZIO[Memcached, MemcachedError, F[R1, R2, R3]]
  }

  trait ResultOutputBuilder extends ResultBuilder {
    def returning[R: Output]: ZIO[Memcached, MemcachedError, R]
  }
}
