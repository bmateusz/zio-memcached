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

import zio.Chunk

trait SortedSets {
  sealed trait Aggregate extends Product { self =>
    private[memcached] final def stringify: String =
      self match {
        case Aggregate.Max => "MAX"
        case Aggregate.Min => "MIN"
        case Aggregate.Sum => "SUM"
      }
  }

  object Aggregate {
    case object Sum extends Aggregate
    case object Min extends Aggregate
    case object Max extends Aggregate
  }

  case object Changed {
    private[memcached] def stringify: String = "CH"
  }

  type Changed = Changed.type

  case object Increment {
    private[memcached] def stringify: String = "INCR"
  }

  type Increment = Increment.type

  sealed trait LexMaximum { self =>
    private[memcached] final def stringify: String =
      self match {
        case LexMaximum.Unbounded     => "+"
        case LexMaximum.Open(value)   => s"($value"
        case LexMaximum.Closed(value) => s"[$value"
      }
  }

  object LexMaximum {
    case object Unbounded                   extends LexMaximum
    sealed case class Open(value: String)   extends LexMaximum
    sealed case class Closed(value: String) extends LexMaximum
  }

  sealed trait LexMinimum { self =>
    private[memcached] final def stringify: String =
      self match {
        case LexMinimum.Unbounded     => "-"
        case LexMinimum.Open(value)   => s"($value"
        case LexMinimum.Closed(value) => s"[$value"
      }
  }

  object LexMinimum {
    case object Unbounded                   extends LexMinimum
    sealed case class Open(value: String)   extends LexMinimum
    sealed case class Closed(value: String) extends LexMinimum
  }

  sealed case class LexRange(min: LexMinimum, max: LexMaximum)

  sealed case class MemberScore[+M](score: Double, member: M)

  type MemberScores[+M] = Chunk[MemberScore[M]]

  sealed trait ScoreMaximum { self =>
    private[memcached] final def stringify: String =
      self match {
        case ScoreMaximum.Infinity      => "+inf"
        case ScoreMaximum.Open(value)   => s"($value"
        case ScoreMaximum.Closed(value) => s"$value"
      }
  }

  object ScoreMaximum {
    case object Infinity                    extends ScoreMaximum
    sealed case class Open(value: Double)   extends ScoreMaximum
    sealed case class Closed(value: Double) extends ScoreMaximum
  }

  sealed trait ScoreMinimum { self =>
    private[memcached] final def stringify: String =
      self match {
        case ScoreMinimum.Infinity      => "-inf"
        case ScoreMinimum.Open(value)   => s"($value"
        case ScoreMinimum.Closed(value) => s"$value"
      }
  }

  object ScoreMinimum {
    case object Infinity                    extends ScoreMinimum
    sealed case class Open(value: Double)   extends ScoreMinimum
    sealed case class Closed(value: Double) extends ScoreMinimum
  }

  sealed case class ScoreRange(min: ScoreMinimum, max: ScoreMaximum)

  case object WithScores {
    private[memcached] def stringify: String = "WITHSCORES"
  }

  type WithScores = WithScores.type

}
