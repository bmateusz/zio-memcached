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

package zio.memcached.model

private[memcached] object ValueHeaders {
  type MetaValueHeader = Map[Char, String]

  sealed trait GenericValueHeader {
    def bytes: Int
  }

  def valueHeader(key: String, flags: String, bytes: String, optCasUnique: Option[String]): GenericValueHeader =
    optCasUnique match {
      case Some(cas) =>
        ValueHeaderWithCas(key, flags.toInt, bytes.toInt, new CasUnique(cas.toLong))
      case None =>
        ValueHeader(key, flags.toInt, bytes.toInt)
    }

  final case class ValueHeader(key: String, flags: Int, bytes: Int) extends GenericValueHeader

  final case class ValueHeaderWithCas(key: String, flags: Int, bytes: Int, casUnique: CasUnique)
      extends GenericValueHeader
}
