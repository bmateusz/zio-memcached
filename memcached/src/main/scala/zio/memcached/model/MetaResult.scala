package zio.memcached.model

import zio.memcached.model.ValueHeaders.MetaValueHeader

object MetaResult {
  sealed trait MetaGetResult[+A] {
    def headers: MetaValueHeader

    def getValue: Option[A]
  }

  /**
   * Successful result of a get command, including the value and the headers. Expect this output if you requested the
   * value with the [[zio.memcached.model.MetaGetFlags.ReturnItemValue]] flag.
   *
   * @param value
   *   the value
   * @param headers
   *   the headers of the value
   * @tparam A
   *   the type of the value
   */
  case class MetaGetResultValue[A](value: A, headers: MetaValueHeader) extends MetaGetResult[A] {
    override def getValue: Option[A] = Some(value)
  }

  /**
   * Successful result of a get command, including the headers only.
   *
   * @param headers
   *   the headers of the value
   * @tparam A
   *   the type of the value (not used, nothing to decode)
   */
  case class MetaGetResultHeadersOnly[A](headers: MetaValueHeader) extends MetaGetResult[A] {
    override def getValue: Option[A] = None
  }

  /**
   * Failure result of a get command, the value does not exist.
   *
   * @tparam A
   *   the type of the value (not used, nothing to decode)
   */
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

  sealed trait MetaDebugResult {
    def headers: Map[String, String]
  }

  case class MetaDebugResultSuccess(headers: Map[String, String]) extends MetaDebugResult

  case object MetaDebugResultNotFound extends MetaDebugResult {
    override def headers: Map[String, String] = Map.empty
  }

}
