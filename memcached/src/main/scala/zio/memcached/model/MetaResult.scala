package zio.memcached.model

import zio.memcached.model.ValueHeaders.MetaValueHeader

object MetaResult {
  sealed trait MetaGetResult[+A] {
    def headers: MetaValueHeader

    def getValue: Option[A]
  }

  case class MetaGetResultSingle[A](value: A, headers: MetaValueHeader) extends MetaGetResult[A] {
    override def getValue: Option[A] = Some(value)
  }

  case class MetaGetResultFlagsOnly[A](headers: MetaValueHeader) extends MetaGetResult[A] {
    override def getValue: Option[A] = None
  }

  case class MetaGetResultNotFound[A]() extends MetaGetResult[A] {
    override def headers: MetaValueHeader = Map.empty

    override def getValue: Option[A] = None
  }

  sealed trait MetaSetResult {
    def headers: MetaValueHeader
  }

  case class MetaSetResultStored(headers: MetaValueHeader) extends MetaSetResult

  case class MetaSetResultNotStored(headers: MetaValueHeader) extends MetaSetResult

  case class MetaSetResultExists(headers: MetaValueHeader) extends MetaSetResult

  case class MetaSetResultNotFound(headers: MetaValueHeader) extends MetaSetResult

  sealed trait MetaDeleteResult {
    def headers: MetaValueHeader
  }

  case class MetaDeleteResultDeleted(headers: MetaValueHeader) extends MetaDeleteResult

  case class MetaDeleteResultNotFound(headers: MetaValueHeader) extends MetaDeleteResult

  case class MetaDeleteResultExists(headers: MetaValueHeader) extends MetaDeleteResult

  sealed trait MetaArithmeticResult {
    def headers: MetaValueHeader

    val value: Option[Long]
  }

  case class MetaArithmeticResultSuccess(headers: MetaValueHeader, value: Option[Long]) extends MetaArithmeticResult

  case class MetaArithmeticResultExists(headers: MetaValueHeader, value: Option[Long]) extends MetaArithmeticResult

  case class MetaArithmeticResultNotFound(headers: MetaValueHeader) extends MetaArithmeticResult {
    override val value: Option[Long] = None
  }

  case class MetaArithmeticResultNotStored(headers: MetaValueHeader) extends MetaArithmeticResult {
    override val value: Option[Long] = None
  }

}
