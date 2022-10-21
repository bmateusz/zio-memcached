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
import zio.stream.{Stream, ZStream}

import java.io.{EOFException, IOException}
import java.net.{InetSocketAddress, SocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, Channel, CompletionHandler}

private[memcached] trait ByteStream {
  def read: Stream[IOException, Byte]
  def write(chunk: Chunk[Byte]): IO[IOException, Option[Unit]]
}

private[memcached] object ByteStream {
  lazy val customized: ZLayer[MemcachedConfig, MemcachedError.IOError, Chunk[ByteStream]] =
    ZLayer.scoped {
      for {
        config  <- ZIO.service[MemcachedConfig]
        service <- Chunk.from(config.nodes).mapZIO(node => connect(new InetSocketAddress(node.host, node.port)))
      } yield service
    }

  lazy val default: ZLayer[Any, MemcachedError.IOError, Chunk[ByteStream]] =
    ZLayer.succeed(MemcachedConfig.Default) >>> customized

  private[this] final val ResponseBufferSize = 1024

  private[this] def closeWith[A](channel: Channel)(op: CompletionHandler[A, Any] => Any): IO[IOException, A] =
    ZIO.asyncInterrupt { k =>
      op(completionHandler(k))
      Left(ZIO.attempt(channel.close()).ignore)
    }

  private[this] def connect(address: => SocketAddress): ZIO[Scope, MemcachedError.IOError, Connection] =
    (for {
      address     <- ZIO.succeed(address)
      makeBuffer   = ZIO.succeed(ByteBuffer.allocateDirect(ResponseBufferSize))
      readBuffer  <- makeBuffer
      writeBuffer <- makeBuffer
      channel     <- openChannel(address)
    } yield new Connection(readBuffer, writeBuffer, channel)).mapError(MemcachedError.IOError)

  private[this] def completionHandler[A](k: IO[IOException, A] => Unit): CompletionHandler[A, Any] =
    new CompletionHandler[A, Any] {
      def completed(result: A, u: Any): Unit = k(ZIO.succeedNow(result))

      def failed(t: Throwable, u: Any): Unit =
        t match {
          case e: IOException => k(ZIO.fail(e))
          case _              => k(ZIO.die(t))
        }
    }

  private[this] def openChannel(address: SocketAddress): ZIO[Scope, IOException, AsynchronousSocketChannel] =
    ZIO.fromAutoCloseable {
      for {
        channel <- ZIO.attempt {
                     val channel = AsynchronousSocketChannel.open()
                     channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
                     channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
                     channel
                   }
        _ <- closeWith[Void](channel)(channel.connect(address, null, _))
        _ <- ZIO.logInfo("Connected to the memcached server.")
      } yield channel
    }.refineToOrDie[IOException]

  private[this] final class Connection(
    readBuffer: ByteBuffer,
    writeBuffer: ByteBuffer,
    channel: AsynchronousSocketChannel
  ) extends ByteStream {

    val read: Stream[IOException, Byte] =
      ZStream.repeatZIOChunkOption {
        val receive =
          for {
            _ <- ZIO.succeed(readBuffer.clear())
            _ <- closeWith[Integer](channel)(channel.read(readBuffer, null, _)).filterOrFail(_ >= 0)(new EOFException())
            chunk <- ZIO.succeed {
                       readBuffer.flip()
                       val count = readBuffer.remaining()
                       val array = Array.ofDim[Byte](count)
                       readBuffer.get(array)
                       Chunk.fromArray(array)
                     }
          } yield chunk

        receive.mapError {
          case _: EOFException => None
          case e: IOException  => Some(e)
        }
      }

    def write(chunk: Chunk[Byte]): IO[IOException, Option[Unit]] =
      ZIO.when(chunk.nonEmpty) {
        ZIO.suspendSucceed {
          writeBuffer.clear()
          val (c, remainder) = chunk.splitAt(writeBuffer.capacity())
          writeBuffer.put(c.toArray)
          writeBuffer.flip()

          closeWith[Integer](channel)(channel.write(writeBuffer, null, _))
            .repeatWhile(_ => writeBuffer.hasRemaining)
            .zipRight(write(remainder))
            .map(_.getOrElse(()))
        }
      }
  }
}
