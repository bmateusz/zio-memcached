package zio.memcached.model

object ValueHeaders {
  type MetaValueHeader = Map[Char, String]

  sealed trait GenericValueHeader

  def valueHeader(key: String, flags: String, bytes: String, casUnique: String): GenericValueHeader =
    Option(casUnique) match {
      case Some(value) =>
        ValueHeaderWithCas(key, flags.toInt, bytes.toInt, new CasUnique(value.toLong))
      case None =>
        ValueHeader(key, flags.toInt, bytes.toInt)
    }

  final case class ValueHeader(key: String, flags: Int, bytes: Int) extends GenericValueHeader

  final case class ValueHeaderWithCas(key: String, flags: Int, bytes: Int, casUnique: CasUnique)
      extends GenericValueHeader
}
