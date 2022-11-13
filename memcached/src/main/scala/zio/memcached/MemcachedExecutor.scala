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
import zio.memcached.Input.Input
import zio.stm.TRef

import java.net.InetSocketAddress

trait MemcachedExecutor {
  def execute(hash: Int, command: Input): IO[MemcachedError, RespValue]
}

object MemcachedExecutor {
  lazy val layer: ZLayer[MemcachedConfig, MemcachedError.IOError, MemcachedExecutor] =
    ZLayer.scoped {
      for {
        config <- ZIO.service[MemcachedConfig]
        node <- Chunk.from(config.nodes).mapZIOPar { configNode =>
                  for {
                    byteStream <- NodeDefinition(configNode)
                    _          <- byteStream.run.forkScoped
                    _          <- ZIO.logInfo(s"Registered node ${configNode.host}:${configNode.port}")
                  } yield byteStream
                }
        _ <- ZIO.logInfo(s"Registered ${config.nodes.size} nodes")
      } yield new Live(node)
    }

  lazy val local: ZLayer[Any, MemcachedError.IOError, MemcachedExecutor] =
    ZLayer.succeed(MemcachedConfig.Default) >>> layer

  lazy val test: ULayer[MemcachedExecutor] =
    TestExecutor.layer

  private[memcached] final case class Request(
    command: Chunk[Byte],
    promise: Promise[MemcachedError, RespValue]
  ) {
    def commandAsString: String = command.map { c =>
      if (c == 13) "\\r"
      else if (c == 10) "\\n"
      else c.toChar
    }.mkString
  }

  private[this] final val RequestQueueSize = 16

  private[this] final class Live(nodes: Chunk[NodeDefinition]) extends MemcachedExecutor {
    private val length = nodes.length

    def execute(hash: Int, command: Input): IO[MemcachedError, RespValue] =
      Promise
        .make[MemcachedError, RespValue]
        .flatMap { promise =>
          (nodes(Math.abs(hash % length)).offer(Request(command, promise)) *> promise.await)
            .onInterrupt(promise.fail(MemcachedError.Interrupted))
        }
  }

  private[this] object NodeDefinition {
    private def singleStreamedExecutor(address: InetSocketAddress): ZIO[Scope, MemcachedError.IOError, Node] =
      for {
        byteStream <- ByteStream.connect(address)
        reqQueue   <- Queue.bounded[Request](RequestQueueSize)
        resQueue   <- Queue.unbounded[Promise[MemcachedError, RespValue]]
        live        = new Node(reqQueue, resQueue, byteStream)
      } yield live

    def apply(nodeConfig: MemcachedConfig.MemcachedNode): ZIO[Scope, MemcachedError.IOError, NodeDefinition] = {
      val address = new InetSocketAddress(nodeConfig.host, nodeConfig.port)
      for {
        initializedNode <- singleStreamedExecutor(address).mapBoth(_ => Option.empty[Node], s => Option(s)).merge
        node            <- TRef.make(initializedNode).commit
      } yield new NodeDefinition(address, node)
    }
  }

  private[this] final class NodeDefinition(address: InetSocketAddress, node: TRef[Option[Node]]) {
    private val runUntilFailure =
      for {
        optLive <- node.get.commit
        live <- optLive match {
                  case Some(value) =>
                    ZIO.succeed(value)
                  case None =>
                    for {
                      live <- NodeDefinition.singleStreamedExecutor(address)
                      _    <- node.set(Some(live)).commit
                    } yield live
                }
        running <- live.run.ignore
        _       <- node.set(Option.empty[Node]).commit
      } yield running

    val run: ZIO[Scope, MemcachedError, Nothing] =
      runUntilFailure
        .retry(Schedule.linear(1.second).jittered)
        .forever

    def offer(request: Request): IO[MemcachedError.NotConnected, Boolean] =
      node.get.commit.flatMap {
        case Some(node) => node.offer(request)
        case None       => ZIO.fail(MemcachedError.NotConnected(s"Node $address is not connected"))
      }
  }

  private[this] final class Node(
    reqQueue: Queue[Request],
    resQueue: Queue[Promise[MemcachedError, RespValue]],
    byteStream: ByteStream
  ) {

    def offer(request: Request): UIO[Boolean] =
      reqQueue.offer(request)

    private def drainWith(e: MemcachedError): UIO[Unit] = resQueue.takeAll.flatMap(ZIO.foreachDiscard(_)(_.fail(e)))

    /**
     * The main loop of the executor. It reads requests from the request queue, sends them to the server in order.
     */
    private val send: IO[MemcachedError.IOError, Unit] =
      reqQueue
        .takeBetween(1, RequestQueueSize)
        .flatMap { reqs =>
          ZIO.foreachDiscard(reqs) { req =>
            ZIO.unlessZIO(req.promise.isDone) {
              byteStream
                .write(req.command)
                .tapBoth(
                  e => req.promise.fail(e),
                  _ => resQueue.offer(req.promise)
                )
            }
          }
        }
        .forever

    /**
     * Receive responses from the server and fulfill the promises.
     */
    private val receive: IO[MemcachedError, Unit] =
      byteStream.read
        .via(RespValue.decoder)
        .collectSome
        .foreach(response => ZIO.logInfo(s"Received $response") *> resQueue.take.flatMap(_.succeed(response)))

    /**
     * Opens a connection to the server and launches send and receive operations. All failures are retried by opening a
     * new connection. Only exits by interruption or defect.
     */
    def run: ZIO[Scope, MemcachedError, Unit] =
      (send race receive)
        .tapBoth(
          e => ZIO.logError(s"Connection failed with error: $e") *> drainWith(e),
          _ => ZIO.logInfo("Connection closed") *> drainWith(MemcachedError.ConnectionClosed)
        )
  }
}
