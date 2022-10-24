package zio.memcached

import zio._
import zio.memcached.Input.{EncodedCommand, ThirtyDaysInSeconds}
import zio.memcached.RespValue.BulkString
import zio.memcached.TestExecutor.KeyInfo
import zio.memcached.model.CasUnique
import zio.memcached.model.ValueHeaders.{ValueHeader, ValueHeaderWithCas}
import zio.stm._

import java.time.Instant

private[memcached] final class TestExecutor(
                                             state: TMap[String, KeyInfo],
                                             casCounter: TRef[Long]
                                           ) extends MemcachedExecutor {
  override def execute(hash: Int, command: EncodedCommand): IO[MemcachedError, RespValue] = {
    val line = command.asString
    val value = command match {
      case Input.Command(_) => None
      case Input.CommandAndValue(_, value) => Some(value)
    }
    val args = line.split(' ')

    val cmd = args.head
    cmd match {
      case "get" | "gets" | "gat" | "gats" =>
        val withCas = cmd.endsWith("s")
        val (key, andTouch) =
          if (cmd == "gat" || cmd == "gats")
            (args(2), Some(args(1).toLong))
          else
            (args(1), None)

        for {
          result <- executeGet(key, withCas)
          _ <- andTouch match {
            case Some(seconds) => executeTouch(key, seconds)
            case None => ZIO.unit
          }
        } yield result

      case "touch" =>
        val key = args(1)
        val seconds = args(2).toLong
        executeTouch(key, seconds)

      case "delete" =>
        val key = args(1)
        executeDelete(key)

      case "set" | "add" | "replace" | "append" | "prepend" | "cas" =>
        val key = args(1)
        val flags = Some(args(2).toInt)
        val exp = Some(args(3).toLong)
        // val bytes = args(4).toInt
        val casUnique = if (cmd == "cas") args(5).toLong else 0L
        cmd match {
          case "set" =>
            executeSet(key, flags, exp, _ => None, _ => value.get.value)
          case "add" =>
            executeSet(key, flags, exp, {
              case Some(_) => Some(RespValue.NotStored)
              case None => None
            }, _ => value.get.value)
          case "replace" =>
            executeSet(key, flags, exp, {
              case Some(_) => None
              case None => Some(RespValue.NotStored)
            }, _ => value.get.value)
          case "append" =>
            executeSet(key, flags, exp, {
              case Some(_) => None
              case None => Some(RespValue.NotStored)
            }, original => original ++ value.get.value)
          case "prepend" =>
            executeSet(key, flags, exp, {
              case Some(_) => None
              case None => Some(RespValue.NotStored)
            }, original => value.get.value ++ original)
          case "cas" =>
            executeSet(key, flags, exp, {
              case Some(original) if original.casUnique == casUnique => None
              case Some(_) => Some(RespValue.Exists)
              case None => Some(RespValue.NotFound)
            }, _ => value.get.value)
        }

      case "incr" | "decr" =>
        val key = args(1)
        val amount = args(2).toLong
        executeIncrement(key, if (cmd == "incr") amount else -amount)

      case command =>
        ZIO.succeedNow(RespValue.Error(s"$command not implemented"))
    }
  }

  private def stateGetHandlingExpiry(key: String): STM[MemcachedError, Option[KeyInfo]] =
    state.get(key).flatMap {
      case Some(keyInfo) if keyInfo.expireAt.exists(_.isBefore(Instant.now())) =>
        state.delete(key).as(None)
      case other =>
        STM.succeed(other)
    }

  private def executeGet(key: String, withCas: Boolean) =
    stateGetHandlingExpiry(key)
      .map {
        case Some(value) =>
          RespValue.BulkStringWithHeader(
            if (withCas)
              ValueHeaderWithCas(
                key,
                value.flags,
                value.value.length,
                CasUnique(value.casUnique)
              )
            else
              ValueHeader(
                key,
                value.flags,
                value.value.length
              )
            ,
            BulkString(value.value)
          )
        case None =>
          RespValue.End // means NotFound
      }
      .commit

  private def executeTouch(key: String, seconds: Long) =
    stateGetHandlingExpiry(key)
      .flatMap {
        case Some(value) =>
          state.put(
            key,
            value.copy(
              expireAt = secondsToExpiration(seconds)
            )
          )
        case None =>
          STM.succeedNow(())
      }
      .commit
      .map(_ => RespValue.Touched)

  private def executeDelete(key: String) =
    stateGetHandlingExpiry(key)
      .flatMap {
        case Some(_) =>
          state
            .delete(key)
            .map(_ => RespValue.Deleted)
        case None =>
          STM.succeedNow(
            RespValue.NotFound
          )
      }
      .commit

  def updateState(key: String, flags: Int, instant: Option[Instant], value: Chunk[Byte]): ZSTM[Any, Nothing, RespValue.Stored.type] =
    casCounter
      .updateAndGet(_ + 1)
      .flatMap { casUnique =>
        state
          .put(
            key,
            KeyInfo(
              value,
              expireAt = instant,
              flags,
              casUnique
            )
          )
      }
      .map(_ => RespValue.Stored)

  private def executeSet(key: String,
                         flags: Option[Int],
                         seconds: Option[Long],
                         decision: Option[KeyInfo] => Option[RespValue],
                         valueTransform: Chunk[Byte] => Chunk[Byte]) =
    stateGetHandlingExpiry(key)
      .flatMap { original =>
        decision(original) match {
          case Some(RespValue.Numeric(value)) =>
            STM.succeedNow(RespValue.Numeric(value))
          case Some(value) =>
            STM.succeedNow(value)
          case None =>
            updateState(
              key,
              flags.orElse(original.map(_.flags)).getOrElse(0),
              seconds.flatMap(secondsToExpiration),
              valueTransform(original.map(_.value).getOrElse(Chunk.empty))
            )
        }
      }
      .commit

  private def executeIncrement(key: String, amount: Long) =
    stateGetHandlingExpiry(key)
      .flatMap {
        case Some(original) =>
          val number = new String(original.value.toArray).toLong
          val result = Math.max(0, number + amount)
          updateState(
            key,
            original.flags,
            original.expireAt,
            Chunk.fromArray(result.toString.getBytes)
          )
            .map(_ => RespValue.Numeric(result))
        case None =>
          STM.succeedNow(RespValue.NotFound)
      }
      .commit

  private def secondsToExpiration(seconds: Long): Option[Instant] =
    if (seconds == 0)
      None
    else if (seconds <= ThirtyDaysInSeconds)
      Some(Instant.now().plusSeconds(seconds))
    else // seconds > 30 days
      Some(Instant.ofEpochSecond(seconds))
}

private[memcached] object TestExecutor {

  final case class KeyInfo(value: Chunk[Byte],
                           expireAt: Option[Instant],
                           flags: Int,
                           casUnique: Long)

  lazy val layer: ULayer[MemcachedExecutor] =
    ZLayer {
      for {
        state <- TMap.empty[String, KeyInfo].commit
        casCounter <- TRef.make(0L).commit
      } yield new TestExecutor(
        state,
        casCounter
      )
    }
}