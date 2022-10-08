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

trait Geo {
  this: Shared =>

  sealed case class LongLat(longitude: Double, latitude: Double)

  sealed case class GeoView(member: String, dist: Option[Double], hash: Option[Long], longLat: Option[LongLat])

  sealed trait RadiusUnit { self =>
    private[memcached] final def stringify: String =
      self match {
        case RadiusUnit.Meters     => "m"
        case RadiusUnit.Kilometers => "km"
        case RadiusUnit.Feet       => "ft"
        case RadiusUnit.Miles      => "mi"
      }
  }

  object RadiusUnit {
    case object Meters     extends RadiusUnit
    case object Kilometers extends RadiusUnit
    case object Feet       extends RadiusUnit
    case object Miles      extends RadiusUnit
  }

  sealed trait StoreOptions {
    def store: Option[Store]
    def stomemcachedt: Option[Stomemcachedt]
  }
  case class StoreResults(results: Store) extends StoreOptions {
    override def store: Option[Store]         = Some(results)
    override def stomemcachedt: Option[Stomemcachedt] = None
  }
  case class Stomemcachedtances(distances: Stomemcachedt) extends StoreOptions {
    override def store: Option[Store]         = None
    override def stomemcachedt: Option[Stomemcachedt] = Some(distances)
  }
  case class StoreBoth(results: Store, distances: Stomemcachedt) extends StoreOptions {
    override def store: Option[Store]         = Some(results)
    override def stomemcachedt: Option[Stomemcachedt] = Some(distances)
  }

  sealed case class Stomemcachedt(key: String)

  case object WithCoord {
    private[memcached] def stringify: String = "WITHCOORD"
  }

  type WithCoord = WithCoord.type

  case object WithDist {
    private[memcached] def stringify: String = "WITHDIST"
  }

  type WithDist = WithDist.type

  case object WithHash {
    private[memcached] def stringify: String = "WITHHASH"
  }

  type WithHash = WithHash.type

}
