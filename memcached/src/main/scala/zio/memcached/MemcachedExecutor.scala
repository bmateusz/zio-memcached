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
  def execute(key: String, command: Input): IO[MemcachedError, RespValue]
}

object MemcachedExecutor {
  lazy val layer: ZLayer[MemcachedConfig, MemcachedError.IOError, MemcachedExecutor] =
    ZLayer.scoped {
      for {
        config <- ZIO.service[MemcachedConfig]
        node <- Chunk.fromIterable(config.nodes).mapZIOPar { configNode =>
                  for {
                    byteStream <- NodeDefinition(configNode, config)
                    _          <- ZIO.logInfo(s"Registered node ${configNode.host}:${configNode.port}")
                  } yield byteStream
                }
        _ <- ZIO.logInfo(s"Registered ${config.nodes.size} nodes")
      } yield new Live(node, config)
    }

  lazy val local: ZLayer[Any, MemcachedError.IOError, MemcachedExecutor] =
    ZLayer.succeed(MemcachedConfig.Default) >>> layer

  lazy val test: ULayer[MemcachedExecutor] =
    TestExecutor.layer

  private[memcached] final case class Request(
    command: Chunk[Byte],
    promise: Promise[MemcachedError, RespValue]
  )

  private[this] final val RequestQueueSize = 16

  private[this] final class Live(nodes: Chunk[NodeDefinition], config: MemcachedConfig) extends MemcachedExecutor {
    private val length = nodes.length

    def execute(key: String, command: Input): IO[MemcachedError, RespValue] =
      Promise
        .make[MemcachedError, RespValue]
        .flatMap { promise =>
          (nodes(Math.abs(config.hashAlgorithm(key) % length)).offer(Request(command, promise)) *> promise.await)
            .onInterrupt(promise.fail(MemcachedError.Interrupted))
        }
  }

  private[this] object NodeDefinition {
    private def streamedExecutor(
      address: InetSocketAddress,
      config: MemcachedConfig
    ): ZIO[Scope, MemcachedError.IOError, Node] =
      for {
        byteStream <- ByteStream.connect(address)
        reqQueue   <- Queue.bounded[Request](config.messageQueueSize)
        resQueue   <- Queue.unbounded[Promise[MemcachedError, RespValue]]
        live        = new Node(reqQueue, resQueue, byteStream)
      } yield live

    private def optionStreamExecutor(
      address: InetSocketAddress,
      config: MemcachedConfig
    ): ZIO[Scope, MemcachedError.IOError, Option[Node]] =
      streamedExecutor(address, config).mapBoth(_ => Option.empty[Node], s => Option(s)).merge

    def apply(
      nodeConfig: MemcachedConfig.MemcachedNode,
      config: MemcachedConfig
    ): ZIO[Scope, MemcachedError.IOError, NodeDefinition] = {
      val address = new InetSocketAddress(nodeConfig.host, nodeConfig.port)
      for {
        initializedNode <- optionStreamExecutor(address, config)
        node            <- TRef.make(initializedNode).commit
        result           = new NodeDefinition(address, node, config)
        _ <- initializedNode match {
               case Some(runningNode) =>
                 result.run(runningNode).forkScoped
               case None =>
                 result.retryLoop.forkScoped
             }
      } yield result
    }
  }

  private[this] final class NodeDefinition(
    address: InetSocketAddress,
    node: TRef[Option[Node]],
    config: MemcachedConfig
  ) {
    private val retrySchedule =
      Schedule.spaced(1.second).zip[Any, Any, Long](Schedule.recurs(30))

    private val retryLoop =
      (for {
        optOldExecutor <- node.getAndSet(Option.empty[Node]).commit
        _              <- ZIO.fromOption(optOldExecutor).map(_.shutdown).orElse(ZIO.unit).fork
        result         <- NodeDefinition.streamedExecutor(address, config).retry(retrySchedule)
        _              <- node.set(Some(result)).commit &> result.run.ignore
      } yield result).forever
        .orElseFail(MemcachedError.Unavailable(s"Node $address is not available"))
        .tapError(e => ZIO.logError(e.message))

    def run(runningNode: Node): ZIO[Scope, MemcachedError, Unit] =
      runningNode.run
        .orElse(retryLoop)

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

    def shutdown: UIO[Unit] =
      reqQueue.takeAll.flatMap { reqs =>
        ZIO.foreachDiscard(reqs) { req =>
          req.promise.fail(MemcachedError.NotConnected("Node is shutting down"))
        }
      }
        .zipParRight(reqQueue.shutdown)

    private def drainWith(e: MemcachedError): UIO[Unit] = resQueue.takeAll.flatMap(ZIO.foreachDiscard(_)(_.fail(e)))

    /**
     * The main loop of the executor. It reads requests from the request queue, sends them to the server in order.
     */
    private val send: IO[MemcachedError, Unit] =
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
        .foreach(response => resQueue.take.flatMap(_.succeed(response)))
        .orDie

    /**
     * Opens a connection to the server and launches send and receive operations. All failures are retried by opening a
     * new connection. Only exits by interruption or defect.
     */
    val run: ZIO[Scope, MemcachedError, Unit] =
      (send race receive)
        .tapBoth(
          e => ZIO.logError(s"Connection failed with error: $e") *> drainWith(e),
          _ => ZIO.logInfo("Connection closed") *> drainWith(MemcachedError.ConnectionClosed)
        )
  }
}
