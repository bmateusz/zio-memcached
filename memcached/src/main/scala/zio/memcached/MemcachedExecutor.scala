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

trait MemcachedExecutor {
  def execute(hash: Int, command: Chunk[RespValue.BulkString]): IO[MemcachedError, RespValue]
}

object MemcachedExecutor {
  lazy val layer: ZLayer[MemcachedConfig, MemcachedError.IOError, MemcachedExecutor] =
    ByteStream.customized >>> StreamedExecutor

  lazy val local: ZLayer[Any, MemcachedError.IOError, MemcachedExecutor] =
    ByteStream.default >>> StreamedExecutor

  // lazy val test: ULayer[MemcachedExecutor] = TestExecutor.layer

  private[this] final case class Request(
    command: Chunk[RespValue.BulkString],
    promise: Promise[MemcachedError, RespValue]
  )

  private[this] final val True: Any => Boolean = _ => true

  private[this] final val RequestQueueSize = 16

  private[this] final val StreamedExecutor: ZLayer[Chunk[ByteStream], Nothing, Live] =
    ZLayer.scoped {
      for {
        byteStreams <- ZIO.service[Chunk[ByteStream]]
        nodes       <- byteStreams.mapZIO(singleStreamedExecutor)
        _           <- nodes.mapZIO(_.run.forkScoped)
      } yield new Live(nodes)
    }

  private[this] final def singleStreamedExecutor(byteStream: ByteStream) =
    for {
      reqQueue <- Queue.bounded[Request](RequestQueueSize)
      resQueue <- Queue.unbounded[Promise[MemcachedError, RespValue]]
      live      = new Node(reqQueue, resQueue, byteStream)
    } yield live

  private[this] final class Live(nodes: Chunk[Node]) extends MemcachedExecutor {
    private val length = nodes.length

    def execute(hash: Int, command: Chunk[RespValue.BulkString]): IO[MemcachedError, RespValue] =
      Promise
        .make[MemcachedError, RespValue]
        .flatMap(promise => nodes(hash % length).offer(Request(command, promise)) *> promise.await)
  }

  private[this] final class Node(
    reqQueue: Queue[Request],
    resQueue: Queue[Promise[MemcachedError, RespValue]],
    byteStream: ByteStream
  ) {

    def offer(request: Request): UIO[Boolean] =
      reqQueue.offer(request)

    /**
     * Opens a connection to the server and launches send and receive operations. All failures are retried by opening a
     * new connection. Only exits by interruption or defect.
     */
    val run: IO[MemcachedError, Unit] =
      (send.forever race receive)
        .tapError(e => ZIO.logWarning(s"Reconnecting due to error: $e") *> drainWith(e))
        .retryWhile(True)
        .tapError(e => ZIO.logError(s"Executor exiting: $e"))

    private def drainWith(e: MemcachedError): UIO[Unit] = resQueue.takeAll.flatMap(ZIO.foreachDiscard(_)(_.fail(e)))

    private def send: IO[MemcachedError.IOError, Option[Unit]] =
      reqQueue.takeBetween(1, RequestQueueSize).flatMap { reqs =>
        val buffer = ChunkBuilder.make[Byte]()
        val it     = reqs.iterator

        while (it.hasNext) {
          val req = it.next()
          req.command.foreach { chunk =>
            buffer ++= chunk.serialize
          }
        }

        val bytes = buffer.result()

        byteStream
          .write(bytes)
          .mapError(MemcachedError.IOError.apply)
          .tapBoth(
            e => ZIO.foreachDiscard(reqs)(_.promise.fail(e)),
            _ => ZIO.foreachDiscard(reqs)(req => resQueue.offer(req.promise))
          )
      }

    private def receive: IO[MemcachedError, Unit] =
      byteStream.read
        .mapError(MemcachedError.IOError.apply)
        .via(RespValue.decoder)
        .collectSome
        .foreach(response => resQueue.take.flatMap(_.succeed(response)))
  }
}
