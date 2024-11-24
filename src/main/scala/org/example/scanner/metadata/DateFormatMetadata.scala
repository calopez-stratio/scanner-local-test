package org.example.scanner.metadata

import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.Try

case class DateFormatMetadata(dateFormats: Set[String]) extends ColumnMetadata

object DateFormatMetadata extends ColumnMetadataExtractor {

  val supportedDateSeparators: Seq[String] = Seq(".", "/", "\\", "-", "_", " ", ",")

  private lazy val strDateFormatters: Map[String, DateTimeFormatter] =
    (Seq("dd", "MM", "yyyy").permutations ++ Seq("MM", "yyyy").permutations)
      .flatMap {
        parts =>
          supportedDateSeparators.map {
            separator =>
              val format = parts.mkString(separator)
              (format, DateTimeFormat.forPattern(format))
          }
      }
      .toMap

  private lazy val numDateFormatters: Map[String, DateTimeFormatter] = Seq("yyyyMM", "yyyyMMdd", "yyyyddMM")
    .map(format => (format, DateTimeFormat.forPattern(format)))
    .toMap

  private lazy val (minDateFormatSize, maxDateFormatSize) = {
    val formats = (numDateFormatters.keys ++ strDateFormatters.keys).map(_.length).toList
    (formats.min, formats.max)
  }

  private val now: DateTime = DateTime.now()

  private def extractDateFormatMetadata(
                                         datumStr: String,
                                         dateFormatters: Map[String, DateTimeFormatter]
                                       ): Option[DateFormatMetadata] = {
    val possibleDateFormats = dateFormatters.flatMap {
      case (format, dateFormatter) => Try {
        assert(datumStr.length.equals(format.length))
        val parsedDate = DateTime.parse(datumStr, dateFormatter)
        if (parsedDate.getYear < 1900 || parsedDate.getYear > now.getYear + 200)
          throw new Exception("Year out of range")
        format
      }.toOption
    }
    if (possibleDateFormats.nonEmpty) Some(DateFormatMetadata(possibleDateFormats.toSet)) else None
  }

  override def extractMetadata(datum: Any, colName: String): Option[ColumnMetadata] = {
    if (null == datum || datum.toString.length > maxDateFormatSize || datum.toString.length < minDateFormatSize)
      return None
    datum match {
      case datumStr: String if datumStr.forall(_.isDigit) => extractDateFormatMetadata(datumStr, numDateFormatters)
      case datumStr: String                               => extractDateFormatMetadata(datumStr, strDateFormatters)
      case datumInt: Integer               => extractDateFormatMetadata(datumInt.toString, numDateFormatters)
      case datumLong: Long                 => extractDateFormatMetadata(datumLong.toString, numDateFormatters)
      case datumLong: BigInt               => extractDateFormatMetadata(datumLong.toString, numDateFormatters)
      case datumLong: BigDecimal           => extractDateFormatMetadata(datumLong.toString, numDateFormatters)
      case datumLong: java.math.BigInteger => extractDateFormatMetadata(datumLong.toString, numDateFormatters)
      case datumLong: java.math.BigDecimal => extractDateFormatMetadata(datumLong.toString, numDateFormatters)
      case _                               => None
    }
  }

  override def reduce(metadataList: List[ColumnMetadata]): ColumnMetadata =
    DateFormatMetadata(metadataList.map(_.asInstanceOf[DateFormatMetadata]).map(_.dateFormats).reduce(_ ++ _))

}
