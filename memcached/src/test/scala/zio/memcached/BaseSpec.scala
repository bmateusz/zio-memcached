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
import zio.schema.codec.{Codec, ProtobufCodec}
import zio.schema.{DeriveSchema, Schema}
import zio.test._

import java.time.Instant
import java.util.UUID

trait BaseSpec extends ZIOSpecDefault {
  implicit val codec: Codec = ProtobufCodec

  override def aspects: Chunk[TestAspectAtLeastR[Live]] =
    Chunk.succeed(TestAspect.timeout(10.seconds))

  def instantOf(millis: Long): UIO[Instant] = ZIO.succeed(Instant.now().plusMillis(millis))

  final val genPatternOption: Gen[Any, Option[String]] =
    Gen.option(Gen.constSample(Sample.noShrink("*")))

  final val uuid: UIO[String] =
    ZIO.succeed(UUID.randomUUID().toString)
}

object BaseSpec {

  /**
   * A sample case class for tests
   */
  case class Person(name: String, age: Int)

  implicit val schemaPerson: Schema[Person] = DeriveSchema.gen[Person]
}
