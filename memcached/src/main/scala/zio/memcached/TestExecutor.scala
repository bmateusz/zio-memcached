package zio.memcached

import zio._
import zio.memcached.Input.{EncodedCommand, ThirtyDaysInSeconds}
import zio.memcached.RespValue.{BulkString, MetaDebugResult, MetaResult}
import zio.memcached.TestExecutor.{InvalidNeedsRevalidation, KeyInfo}
import zio.memcached.model.{CasUnique, MetaArithmeticFlags, MetaDeleteFlags, MetaGetFlags, MetaSetFlags}
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
      case Input.Command(_)                => None
      case Input.CommandAndValue(_, value) => Some(value)
    }
    val args = line.split(' ')

    val cmd = args.head
    cmd match {
      case "get" | "gets" | "gat" | "gats" =>
        val withCas = cmd.endsWith("s")
        val (key, newTtl) =
          if (cmd.startsWith("gat"))
            (args(2), Some(args(1).toLong))
          else
            (args(1), None)

        for {
          result <- executeGet(key, withCas)
          _ <- newTtl match {
                 case Some(seconds) => executeTouch(key, seconds)
                 case None          => ZIO.unit
               }
        } yield result

      case "touch" =>
        val key     = args(1)
        val seconds = args(2).toLong
        executeTouch(key, seconds)

      case "delete" =>
        val key = args(1)
        executeDelete(key)

      case "set" | "add" | "replace" | "append" | "prepend" | "cas" =>
        val key   = args(1)
        val flags = Some(args(2).toInt)
        val exp   = Some(args(3).toLong)
        // val bytes = args(4).toInt
        val casUnique = if (cmd == "cas") args(5).toLong else 0L
        executeGenericSet(cmd, key, flags, exp, casUnique, value.get.value)

      case "incr" | "decr" =>
        val key    = args(1)
        val amount = args(2).toLong
        executeIncrement(key, if (cmd == "incr") amount else -amount)

      case "mg" =>
        val key   = args(1)
        val flags = MetaGetFlags.fromString(args.drop(2).mkString(" "))
        executeMetaGet(key, flags)

      case "ms" =>
        val key = args(1)
        // val bytes = args(2)
        val flags = MetaSetFlags.fromString(args.drop(3).mkString(" "))
        executeMetaSet(key, flags, value.get.value)

      case "md" =>
        val key   = args(1)
        val flags = MetaDeleteFlags.fromString(args.drop(2).mkString(" "))
        executeMetaDelete(key, flags)

      case "ma" =>
        val key   = args(1)
        val flags = MetaArithmeticFlags.fromString(args.drop(2).mkString(" "))
        executeMetaArithmetic(key, flags)

      case "me" =>
        val key = args(1)
        executeMetaDebug(key)

      case command =>
        ZIO.succeedNow(RespValue.Error(s"$command not implemented"))
    }
  }

  private def executeGet(key: String, withCas: Boolean) =
    handlingExpiryOf(key) {
      case Some(value) =>
        STM.succeedNow(
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
              ),
            BulkString(value.value)
          )
        )
      case None =>
        STM.succeedNow(RespValue.End) // means NotFound
    }

  private def executeTouch(key: String, seconds: Long) =
    handlingExpiryAndGetNow(key) {
      case (Some(value), now) =>
        state.put(
          key,
          value.copy(
            expireAt = secondsToExpiration(seconds, now)
          )
        )
      case (None, _) =>
        STM.succeedNow(())
    }
      .map(_ => RespValue.Touched)

  private def executeSet(
    key: String,
    flags: Option[Int],
    seconds: Option[Long],
    decision: Option[KeyInfo] => Option[RespValue],
    valueTransform: Chunk[Byte] => Chunk[Byte]
  ) =
    handlingExpiryAndGetNow(key) { case (original, now) =>
      decision(original) match {
        case Some(RespValue.Numeric(value)) =>
          STM.succeedNow(RespValue.Numeric(value))
        case Some(value) =>
          STM.succeedNow(value)
        case None =>
          updateState(
            original,
            key,
            flags.orElse(original.map(_.flags)).getOrElse(0),
            seconds.flatMap(secondsToExpiration(_, now)),
            valueTransform(original.map(_.value).getOrElse(Chunk.empty))
          )
      }
    }

  private def executeGenericSet(
    cmd: String,
    key: String,
    flags: Option[Int],
    exp: Option[Long],
    casUnique: Long,
    value: Chunk[Byte]
  ) =
    cmd match {
      case "set" =>
        executeSet(key, flags, exp, _ => None, _ => value)
      case "add" =>
        executeSet(
          key,
          flags,
          exp,
          {
            case Some(_) => Some(RespValue.NotStored)
            case None    => None
          },
          _ => value
        )
      case "replace" =>
        executeSet(
          key,
          flags,
          exp,
          {
            case Some(_) => None
            case None    => Some(RespValue.NotStored)
          },
          _ => value
        )
      case "append" | "prepend" =>
        executeSet(
          key,
          flags,
          exp,
          {
            case Some(_) => None
            case None    => Some(RespValue.NotStored)
          },
          original => if (cmd == "append") original ++ value else value ++ original
        )
      case "cas" =>
        executeSet(
          key,
          flags,
          exp,
          {
            case Some(original) if original.casUnique == casUnique => None
            case Some(_)                                           => Some(RespValue.Exists)
            case None                                              => Some(RespValue.NotFound)
          },
          _ => value
        )
      case other =>
        ZIO.succeedNow(RespValue.Error(s"$other not implemented in executeGenericSet"))
    }

  private def executeIncrement(key: String, amount: Long) =
    handlingExpiryOf(key) {
      case Some(original) =>
        val number = new String(original.value.toArray).toLong
        val result = Math.max(0, number + amount)
        updateState(
          Some(original),
          key,
          original.flags,
          original.expireAt,
          Chunk.fromArray(result.toString.getBytes)
        )
          .map(_ => RespValue.Numeric(result))
      case None =>
        STM.succeedNow(RespValue.NotFound)
    }

  private def executeDelete(key: String) =
    handlingExpiryOf(key) {
      case Some(_) =>
        state
          .delete(key)
          .map(_ => RespValue.Deleted)
      case None =>
        STM.succeedNow(
          RespValue.NotFound
        )
    }

  private def executeMetaGet(key: String, flags: MetaGetFlags): IO[MemcachedError, RespValue] = {
    def updateIfNeeded(key: String, flags: MetaGetFlags, originalInfo: KeyInfo, now: Instant) = {
      val newExpireAt = flags.collectFirst { case value: MetaGetFlags.UpdateRemainingTTL => value }
        .map(updateRemainingTTL => secondsToExpiration(updateRemainingTTL.seconds, now))
        .getOrElse(originalInfo.expireAt)
      val newValidity = originalInfo.validity match {
        case TestExecutor.Valid                    => TestExecutor.Valid
        case TestExecutor.InvalidNeedsRevalidation => TestExecutor.Invalid
        case TestExecutor.Invalid                  => TestExecutor.Invalid
      }
      val newInfo = originalInfo.copy(expireAt = newExpireAt, validity = newValidity)
      if (newInfo != originalInfo) {
        state
          .put(key, newInfo)
          .as(newInfo)
      } else {
        STM.succeedNow(originalInfo)
      }
    }
    handlingExpiryAndGetNow(key) {
      case (Some(originalInfo), now) =>
        updateIfNeeded(key, flags, originalInfo, now).flatMap { info =>
          val validityHeaders = originalInfo.validity match {
            case TestExecutor.Valid                    => Map.empty[Char, String]
            case TestExecutor.InvalidNeedsRevalidation => Map('X' -> "", 'W' -> "")
            case TestExecutor.Invalid                  => Map('X' -> "", 'Z' -> "")
          }
          val metaHeader = flags.flags.foldLeft(validityHeaders) {
            case (acc, MetaGetFlags.ReturnClientFlagsToken) =>
              acc + ('f' -> info.flags.toString)
            case (acc, MetaGetFlags.ReturnItemTTL) =>
              acc + ('e' -> info.expireAt.map(_.getEpochSecond).getOrElse(-1L).toString)
            case (acc, MetaGetFlags.ReturnItemCasToken) =>
              acc + ('c' -> info.casUnique.toString)
            case (acc, MetaGetFlags.ReturnKeyAsToken) =>
              acc + ('k' -> key)
            case (acc, MetaGetFlags.InterpretKeyAsBase64) =>
              acc + ('b' -> "")
            case (acc, _) =>
              acc
          }

          STM.succeedNow(
            MetaResult(
              RespValue.Stored,
              metaHeader,
              if (flags.contains(MetaGetFlags.ReturnItemValue)) Some(BulkString(info.value)) else None
            )
          )
        }
      case (None, _) =>
        STM.succeedNow(RespValue.MetaResult(RespValue.NotFound, Map.empty, None))
    }
  }

  private def executeMetaDebug(key: String): IO[MemcachedError, RespValue] =
    Clock.instant.flatMap { now =>
      state
        .get(key)
        .flatMap {
          case Some(info) =>
            STM.succeedNow(
              MetaDebugResult(
                Map(
                  "key"  -> key,
                  "exp"  -> info.expireAt.map(now.getEpochSecond - _.getEpochSecond).getOrElse(-1L).toString,
                  "size" -> info.value.length.toString,
                  "cas"  -> info.casUnique.toString,
                  "la" -> info.lastAccessed
                    .map(_.getEpochSecond)
                    .getOrElse(0L)
                    .toString, // it works a bit differently in memcached
                  "fetch" -> (if (info.lastAccessed.isDefined) "yes" else "no"),
                  "cls"   -> "1"
                )
              )
            )

          case None =>
            STM.succeedNow(RespValue.MetaResult(RespValue.NotFound, Map.empty, None))
        }
        .commit
    }

  private def executeMetaArithmetic(key: String, flags: MetaArithmeticFlags): IO[MemcachedError, RespValue] =
    flags.createItemOnMiss match {
      case Some(createItemOnMiss) =>
        val initialValue = Chunk.fromArray(flags.initialValue.map(_.value).getOrElse(0L).toString.getBytes())
        executeGenericSet(
          "add",
          key,
          None,
          Some(createItemOnMiss.seconds),
          flags.casToken.map(_.cas.value).getOrElse(0L),
          initialValue
        ).as(RespValue.MetaResult(RespValue.Exists, Map.empty, Some(BulkString(initialValue))))

      case None =>
        executeIncrement(key, flags.delta.map(_.value).getOrElse(1L)).map {
          case RespValue.Numeric(value) =>
            MetaResult(
              RespValue.Exists,
              Map.empty,
              if (flags.contains(MetaArithmeticFlags.ReturnItemValue))
                Some(RespValue.BulkString(Chunk.fromArray(value.toString.getBytes())))
              else
                None
            )
          case other =>
            MetaResult(
              other,
              Map.empty,
              None
            )
        }
    }

  private def executeMetaSet(key: String, flags: MetaSetFlags, value: Chunk[Byte]): IO[MemcachedError, RespValue] = {
    val clientFlags       = flags.clientFlags.getOrElse(0)
    val providedCasUnique = flags.casUnique.map(_.value)
    val exp               = flags.expiration
    val mode              = flags.mode

    def resultFlags(info: KeyInfo) =
      flags.flags.foldLeft(Map.empty[Char, String]) {
        case (acc, MetaSetFlags.ReturnItemCasToken) =>
          acc + ('c' -> info.casUnique.toString)
        case (acc, MetaSetFlags.ReturnKeyAsToken) =>
          acc + ('k' -> key)
        case (acc, MetaSetFlags.InterpretKeyAsBase64) if flags.flags.contains(MetaSetFlags.ReturnKeyAsToken) =>
          acc + ('b' -> "")
        case (acc, _) =>
          acc
      }

    handlingExpiryAndGetNow(key) {
      case (Some(originalInfo), now) =>
        mode match {
          case MetaSetFlags.ModeAppend | MetaSetFlags.ModePrepend | MetaSetFlags.ModeReplace | MetaSetFlags.ModeSet =>
            providedCasUnique match {
              case Some(providedCas) if providedCas != originalInfo.casUnique =>
                if (flags.contains(MetaSetFlags.InvalidateIfCasIsOlder)) {
                  getNextCasUnique.flatMap { nextCas =>
                    val newInfo = originalInfo.copy(
                      value = value,
                      casUnique = nextCas,
                      validity = TestExecutor.InvalidNeedsRevalidation
                    )
                    state
                      .put(key, newInfo)
                      .as(RespValue.MetaResult(RespValue.Stored, resultFlags(newInfo), None))
                  }
                } else {
                  STM.succeedNow(RespValue.MetaResult(RespValue.Exists, Map.empty, None))
                }
              case _ =>
                getNextCasUnique.flatMap { nextCas =>
                  val newInfo = originalInfo.copy(
                    value = mode match {
                      case MetaSetFlags.ModeAppend  => originalInfo.value ++ value
                      case MetaSetFlags.ModePrepend => value ++ originalInfo.value
                      case MetaSetFlags.ModeReplace => value
                      case MetaSetFlags.ModeSet     => value
                      case _                        => originalInfo.value
                    },
                    flags = clientFlags,
                    expireAt = exp.flatMap(secondsToExpiration(_, now)),
                    casUnique = nextCas,
                    validity = TestExecutor.Valid,
                    lastAccessed = None
                  )
                  state
                    .put(key, newInfo)
                    .as(RespValue.MetaResult(RespValue.Stored, resultFlags(newInfo), None))
                }
            }
          case MetaSetFlags.ModeAdd =>
            STM.succeedNow(RespValue.MetaResult(RespValue.NotFound, Map.empty, None))
        }
      case (None, now) =>
        mode match {
          case MetaSetFlags.ModeAppend | MetaSetFlags.ModePrepend | MetaSetFlags.ModeReplace =>
            STM.succeedNow(RespValue.MetaResult(RespValue.NotStored, Map.empty, None))
          case MetaSetFlags.ModeAdd | MetaSetFlags.ModeSet =>
            getNextCasUnique.flatMap { nextCas =>
              val expireAt = exp.flatMap(secondsToExpiration(_, now))
              val info     = KeyInfo(value, expireAt, clientFlags, nextCas)
              state
                .put(key, info)
                .as(RespValue.MetaResult(RespValue.Stored, resultFlags(info), None))
            }
        }
    }
  }

  private def executeMetaDelete(key: String, flags: MetaDeleteFlags): IO[MemcachedError, RespValue] =
    if (flags.contains(MetaDeleteFlags.Invalidate))
      state
        .get(key)
        .flatMap {
          case Some(info) =>
            state
              .put(key, info.copy(validity = InvalidNeedsRevalidation))
              .as(RespValue.MetaResult(RespValue.Stored, Map.empty, None))
          case None =>
            STM.succeedNow(RespValue.MetaResult(RespValue.NotFound, Map.empty, None))
        }
        .commit
    else
      executeDelete(key).map {
        case RespValue.Deleted => RespValue.MetaResult(RespValue.Stored, Map.empty, None)
        case other             => RespValue.MetaResult(other, Map.empty, None)
      }

  private def handlingExpiryOf[T](key: String)(cont: Option[KeyInfo] => STM[MemcachedError, T]) =
    handlingExpiryAndGetNow(key) { case (info, _) =>
      cont(info)
    }

  private def handlingExpiryAndGetNow[T](key: String)(cont: (Option[KeyInfo], Instant) => STM[MemcachedError, T]) =
    Clock.instant.flatMap { now =>
      state
        .get(key)
        .flatMap {
          case Some(keyInfo) if keyInfo.expireAt.exists(_.isBefore(now)) =>
            state.delete(key).as(None: Option[KeyInfo])
          case Some(keyInfo) =>
            val bumped = keyInfo.copy(lastAccessed = Some(now))
            state.put(key, bumped).as(Some(bumped))
          case None =>
            STM.succeedNow(None: Option[KeyInfo])
        }
        .flatMap(cont(_, now))
        .commit
    }

  private def updateState(
    original: Option[KeyInfo],
    key: String,
    flags: Int,
    instant: Option[Instant],
    value: Chunk[Byte]
  ): ZSTM[Any, Nothing, RespValue.Stored.type] =
    casCounter
      .updateAndGet(_ + 1)
      .flatMap { casUnique =>
        state
          .put(
            key,
            original match {
              case Some(keyInfo) =>
                keyInfo.copy(
                  flags = flags,
                  expireAt = instant,
                  casUnique = casUnique,
                  value = value
                )
              case None =>
                KeyInfo(
                  flags = flags,
                  expireAt = instant,
                  casUnique = casUnique,
                  value = value
                )
            }
          )
      }
      .map(_ => RespValue.Stored)

  private def getNextCasUnique: STM[Nothing, Long] =
    casCounter.getAndUpdate(_ + 1)

  private def secondsToExpiration(seconds: Long, now: Instant): Option[Instant] =
    if (seconds == 0)
      None
    else if (seconds <= ThirtyDaysInSeconds)
      Some(now.plusSeconds(seconds))
    else // seconds > 30 days
      Some(Instant.ofEpochSecond(seconds))
}

private[memcached] object TestExecutor {

  final case class KeyInfo(
    value: Chunk[Byte],
    expireAt: Option[Instant],
    flags: Int,
    casUnique: Long,
    lastAccessed: Option[Instant] = None,
    validity: Validity = Valid
  )

  sealed trait Validity

  case object Valid extends Validity

  case object InvalidNeedsRevalidation extends Validity

  case object Invalid extends Validity

  lazy val layer: ULayer[MemcachedExecutor] =
    ZLayer {
      for {
        state      <- TMap.empty[String, KeyInfo].commit
        casCounter <- TRef.make(0L).commit
      } yield new TestExecutor(
        state,
        casCounter
      )
    }
}
