package zio.memcached.model

import zio.memcached.model.ValueHeaders.MetaValueHeader

/**
 * Meta result. See the "Meta Commands" section in memcached's protocol documentation
 * [[https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt]] for more details.
 */
object MetaResult {

  /**
   * Meta get result base trait.
   */
  sealed trait MetaGetResult[+A] {

    /**
     * Raw headers returned by memcached. The key is a single ascii character and the value is a string.
     */
    def headers: MetaValueHeader

    /**
     * Returns the value decoded or None if it does not exist. The request must contain the
     * [[zio.memcached.model.MetaGetFlags.ReturnItemValue]] flag to get this value.
     */
    def getValue: Option[A]

    /**
     * Returns the compare and swap token. The request must contain the
     * [[zio.memcached.model.MetaGetFlags.ReturnItemCasUnique]] flag to get this value.
     */
    def getCasUnique: Option[CasUnique] = headers.get('c').map(c => CasUnique.apply(c.toLong))

    /**
     * Returns the size of the value. The request must contain the [[zio.memcached.model.MetaGetFlags.ReturnItemSize]]
     * flag to get this value.
     */
    def getSize: Option[Int] = headers.get('s').map(_.toInt)

    /**
     * Returns the client flags of the value. The request must contain the
     * [[zio.memcached.model.MetaGetFlags.ReturnClientFlags]] flag to get this value.
     */
    def getClientFlags: Option[Int] = headers.get('f').map(_.toInt)

    /**
     * Returns the time since the item was last accessed. The request must contain the
     * [[zio.memcached.model.MetaGetFlags.ReturnTimeSinceItemWasLastAccessed]] flag to get this value.
     */
    def getTimeSinceLastAccess: Option[Long] = headers.get('l').map(_.toLong)

    /**
     * Returns whether the item has been accessed before. The request must contain the
     * [[zio.memcached.model.MetaGetFlags.ReturnWhetherItemHasBeenHitBefore]] flag to get this value.
     */
    def getItemHasBeenHitBefore: Option[Boolean] = headers.get('h').map(_ == "1")

    /**
     * Returns the time to live of the item in seconds, -1 for unlimited. The request must contain the
     * [[zio.memcached.model.MetaGetFlags.ReturnItemTTL]] flag to get this value.
     */
    def getItemTTL: Option[Long] = headers.get('t').map(_.toLong)

    /**
     * Returns the key of the item. The request must contain the [[zio.memcached.model.MetaGetFlags.ReturnKeyAsToken]]
     * flag to get this value.
     */
    def getKey: Option[String] = headers.get('k')

    /**
     * Returns whether the key is base64 encoded. The request must contain the
     * [[zio.memcached.model.MetaGetFlags.InterpretKeyAsBase64]] flag to get this value.
     */
    def isKeyBoolean: Boolean = headers.contains('b')

    /**
     * Returns the opaque token. The request must contain the [[zio.memcached.model.MetaGetFlags.Opaque]] flag to get
     * this value.
     */
    def getOpaque: Option[String] = headers.get('O')

    /**
     * Returns whether the item is stale, which means that the item has been marked as stale using meta delete and
     * providing the [[zio.memcached.model.MetaDeleteFlags.Invalidate]] flag.
     */
    def getItemIsStale: Boolean = headers.contains('X')

    /**
     * Returns whether the item won the recache flag, which means the item has been marked as stale using meta delete
     * and providing the [[zio.memcached.model.MetaDeleteFlags.Invalidate]] flag, and your request was the first since
     * that.
     */
    def getClientWonTheRecacheFlag: Boolean = headers.contains('W')

    /**
     * Returns whether the item lost the recache flag, which means the item has been marked as stale using meta delete
     * and providing the [[zio.memcached.model.MetaDeleteFlags.Invalidate]] flag, and your request was *NOT* the first
     * since that.
     */
    def getClientLostTheRecacheFlag: Boolean = headers.contains('Z')
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
