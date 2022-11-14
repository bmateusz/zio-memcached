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
import zio.memcached.MemcachedError.IOError
import zio.stream.{Stream, ZStream}

import java.io.IOException
import java.net.{SocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, Channel, CompletionHandler}

private[memcached] trait ByteStream {
  def read: Stream[MemcachedError.IOError, Byte]
  def write(chunk: Chunk[Byte]): IO[MemcachedError, Unit]
}

private[memcached] object ByteStream {
  private[this] final val ResponseBufferSize = 1024

  def ioErrorHandler(t: Throwable): IO[IOError, Nothing] = t match {
    case e: IOException => ZIO.fail(IOError(e))
    case _              => ZIO.die(t)
  }

  private[this] def closeWith[A](channel: Channel)(op: CompletionHandler[A, Any] => Any): IO[IOError, A] =
    ZIO.asyncInterrupt { k =>
      op(completionHandler(k))
      Left(ZIO.attempt(channel.close()).ignore)
    }

  private[memcached] def connect(address: => SocketAddress): ZIO[Scope, IOError, Connection] =
    for {
      _           <- ZIO.logInfo(s"Connecting to $address")
      address     <- ZIO.succeed(address)
      makeBuffer   = ZIO.succeed(ByteBuffer.allocateDirect(ResponseBufferSize))
      readBuffer  <- makeBuffer
      writeBuffer <- makeBuffer
      channel     <- openChannel(address)
    } yield new Connection(readBuffer, writeBuffer, channel)

  private[this] def completionHandler[A](k: IO[IOError, A] => Unit): CompletionHandler[A, Any] =
    new CompletionHandler[A, Any] {
      def completed(result: A, u: Any): Unit = k(ZIO.succeedNow(result))

      def failed(t: Throwable, u: Any): Unit = k(ioErrorHandler(t))
    }

  private[this] def openChannel(address: SocketAddress): ZIO[Scope, IOError, AsynchronousSocketChannel] =
    ZIO.fromAutoCloseable {
      for {
        channel <- ZIO.attempt {
                     val channel = AsynchronousSocketChannel.open()
                     channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
                     channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
                     channel
                   }.mapError {
                     case e: IOException => MemcachedError.IOError(e)
                     case t              => throw t
                   }
        _ <- closeWith[Void](channel)(channel.connect(address, null, _))
               .tapBoth(
                 e => ZIO.logError(s"Failed to connect to $address: $e"),
                 _ => ZIO.logInfo(s"Connected to the memcached server $address")
               )
      } yield channel
    }

  private[memcached] final class Connection(
    readBuffer: ByteBuffer,
    writeBuffer: ByteBuffer,
    channel: AsynchronousSocketChannel
  ) extends ByteStream {
    override val read: Stream[MemcachedError.IOError, Byte] =
      ZStream.repeatZIOChunk {
        ZIO.asyncInterrupt { (k: IO[MemcachedError.IOError, Chunk[Byte]] => Unit) =>
          readBuffer.clear()
          channel.read(
            readBuffer,
            null,
            new CompletionHandler[Integer, IO[MemcachedError, Chunk[Byte]]] {
              def completed(result: Integer, u: IO[MemcachedError, Chunk[Byte]]): Unit = {
                readBuffer.flip()

                val count = readBuffer.remaining()
                if (count <= 0) {
                  k(ZIO.fail(MemcachedError.IOError(new IOException("Connection closed."))))
                } else {
                  val bytes = Array.ofDim[Byte](count)
                  readBuffer.get(bytes, 0, count)
                  k(ZIO.succeedNow(Chunk.fromArray(bytes)))
                }
              }

              def failed(t: Throwable, u: IO[MemcachedError, Chunk[Byte]]): Unit =
                k(ioErrorHandler(t))
            }
          )
          Left(ZIO.attempt(channel.close()).ignore)
        }
      }

    override def write(chunk: Chunk[Byte]): IO[MemcachedError, Unit] =
      ZIO.suspend {
        writeBuffer.clear()
        val (c, remainder) = chunk.splitAt(writeBuffer.capacity())
        writeBuffer.put(c.toArray)
        writeBuffer.flip()

        closeWith[Integer](channel)(channel.write(writeBuffer, null, _))
          .repeatWhile(_ => writeBuffer.hasRemaining)
          .zipRight(if (remainder.isEmpty) ZIO.unit else write(remainder))
      }.mapError(err => MemcachedError.WriteError(err))
  }
}
