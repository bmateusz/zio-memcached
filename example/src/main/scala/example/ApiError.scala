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

package example

import zhttp.http._

import scala.util.control.NoStackTrace

sealed trait ApiError extends NoStackTrace { self =>
  import ApiError._

  final def toResponse: Response =
    self match {
      case CorruptedData | GithubUnreachable => Response.fromHttpError(HttpError.InternalServerError())
      case CacheMiss                         => Response.text("Cache miss").setStatus(Status.NotFound)
      case CommandNotFound                   => Response.text("Command not found").setStatus(Status.NotFound)
      case MissingMandatoryField(field)      => Response.text(s"Missing mandatory field: $field").setStatus(Status.NotFound)
      case UnknownProject(path)              => Response.fromHttpError(HttpError.NotFound(Path.root / path))
    }
}

object ApiError {
  case object CacheMiss                           extends ApiError
  case object CorruptedData                       extends ApiError
  case object CommandNotFound                     extends ApiError
  case class MissingMandatoryField(field: String) extends ApiError
  case object GithubUnreachable                   extends ApiError
  final case class UnknownProject(path: String)   extends ApiError
}
