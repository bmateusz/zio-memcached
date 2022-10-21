package zio.memcached.api

import zio.ZIO
import zio.memcached.Input._
import zio.memcached.Output._
import zio.memcached.model.MetaArithmeticFlags.MetaArithmeticFlag
import zio.memcached.model.MetaDebugFlags.MetaDebugFlag
import zio.memcached.model.MetaDeleteFlags.MetaDeleteFlag
import zio.memcached.model.MetaGetFlags.MetaGetFlag
import zio.memcached.model.MetaResult.{MetaArithmeticResult, MetaDeleteResult, MetaGetResult, MetaSetResult}
import zio.memcached.model.MetaSetFlags.MetaSetFlag
import zio.memcached.model.{MetaArithmeticFlags, MetaDebugFlags, MetaDeleteFlags, MetaGetFlags, MetaSetFlags}
import zio.memcached.{Memcached, MemcachedCommand, MemcachedError}
import zio.schema.Schema

trait Meta {
  def metaGet[R: Schema](key: String, flags: MetaGetFlag*): ZIO[Memcached, MemcachedError, MetaGetResult[R]] =
    metaGet(key, MetaGetFlags(flags))

  def metaGet[R: Schema](key: String, flags: MetaGetFlags): ZIO[Memcached, MemcachedError, MetaGetResult[R]] =
    MemcachedCommand(new MetaGetCommand(key), MetaGetOutput[R]()).run(flags)

  def metaSet[R: Schema](key: String, value: R, flags: MetaSetFlag*): ZIO[Memcached, MemcachedError, MetaSetResult] =
    metaSet[R](key, value, MetaSetFlags(flags))

  def metaSet[R: Schema](key: String, value: R, flags: MetaSetFlags): ZIO[Memcached, MemcachedError, MetaSetResult] =
    MemcachedCommand(new MetaSetCommand[R](key), MetaSetOutput).run((value, flags))

  def metaDelete(key: String, flags: MetaDeleteFlag*): ZIO[Memcached, MemcachedError, MetaDeleteResult] =
    metaDelete(key, MetaDeleteFlags(flags))

  def metaDelete(key: String, flags: MetaDeleteFlags): ZIO[Memcached, MemcachedError, MetaDeleteResult] =
    MemcachedCommand(new MetaDeleteCommand(key), MetaDeleteOutput).run(flags)

  def metaArithmetic(key: String, flags: MetaArithmeticFlag*): ZIO[Memcached, MemcachedError, MetaArithmeticResult] =
    metaArithmetic(key, MetaArithmeticFlags(flags))

  def metaArithmetic(key: String, flags: MetaArithmeticFlags): ZIO[Memcached, MemcachedError, MetaArithmeticResult] =
    MemcachedCommand(new MetaArithmeticCommand(key), MetaArithmeticOutput).run(flags)

  def metaDebug(key: String, flags: MetaDebugFlag*): ZIO[Memcached, MemcachedError, Option[Map[String, String]]] =
    metaDebug(key, MetaDebugFlags(flags))

  def metaDebug(key: String, flags: MetaDebugFlags): ZIO[Memcached, MemcachedError, Option[Map[String, String]]] =
    MemcachedCommand(new MetaDebugCommand(key), MetaDebugOutput).run(flags)
}
