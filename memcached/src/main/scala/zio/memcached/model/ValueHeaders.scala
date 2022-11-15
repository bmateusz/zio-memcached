package zio.memcached.model

private[memcached] object ValueHeaders {
  type MetaValueHeader = Map[Char, String]

  sealed trait GenericValueHeader {
    def bytes: Int
  }

  def valueHeader(key: String, flags: String, bytes: String, optCasUnique: Option[String]): GenericValueHeader =
    optCasUnique match {
      case Some(cas) =>
        ValueHeaderWithCas(key, flags.toInt, bytes.toInt, new CasUnique(cas.toLong))
      case None =>
        ValueHeader(key, flags.toInt, bytes.toInt)
    }

  final case class ValueHeader(key: String, flags: Int, bytes: Int) extends GenericValueHeader

  final case class ValueHeaderWithCas(key: String, flags: Int, bytes: Int, casUnique: CasUnique)
      extends GenericValueHeader
}
