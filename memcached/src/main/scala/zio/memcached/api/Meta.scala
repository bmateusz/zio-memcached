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
  val command = "mg "

  /*
  mg foo c f h k l Oopaque s t v
  VA 2 c1 f0 h1 kfoo l0 Oopaque s2 t-1
  aa
   */

  def metaGet[R: Schema](key: String, flags: MetaGetFlag*): ZIO[Memcached, MemcachedError, MetaGetResult[R]] =
    metaGet(key, MetaGetFlags(flags))

  def metaGet[R: Schema](key: String, flags: MetaGetFlags): ZIO[Memcached, MemcachedError, MetaGetResult[R]] =
    MemcachedCommand(MetaGetCommand, MetaGetOutput[R]()).run((key, flags))

  def metaSet[R: Schema](key: String, value: R, flags: MetaSetFlag*): ZIO[Memcached, MemcachedError, MetaSetResult] =
    metaSet[R](key, value, MetaSetFlags(flags))

  def metaSet[R: Schema](key: String, value: R, flags: MetaSetFlags): ZIO[Memcached, MemcachedError, MetaSetResult] =
    MemcachedCommand(new MetaSetCommand[R](), MetaSetOutput).run((key, value, flags))

  def metaDelete(key: String, flags: MetaDeleteFlag*): ZIO[Memcached, MemcachedError, MetaDeleteResult] =
    metaDelete(key, MetaDeleteFlags(flags))

  def metaDelete(key: String, flags: MetaDeleteFlags): ZIO[Memcached, MemcachedError, MetaDeleteResult] =
    MemcachedCommand(MetaDeleteCommand, MetaDeleteOutput).run((key, flags))

  def metaArithmetic(key: String, flags: MetaArithmeticFlag*): ZIO[Memcached, MemcachedError, MetaArithmeticResult] =
    metaArithmetic(key, MetaArithmeticFlags(flags))

  def metaArithmetic(key: String, flags: MetaArithmeticFlags): ZIO[Memcached, MemcachedError, MetaArithmeticResult] =
    MemcachedCommand(MetaArithmeticCommand, MetaArithmeticOutput).run((key, flags))

  def metaDebug(key: String, flags: MetaDebugFlag*): ZIO[Memcached, MemcachedError, Option[Map[String, String]]] =
    metaDebug(key, MetaDebugFlags(flags))

  def metaDebug(key: String, flags: MetaDebugFlags): ZIO[Memcached, MemcachedError, Option[Map[String, String]]] =
    MemcachedCommand(MetaDebugCommand, MetaDebugOutput).run((key, flags))
}
