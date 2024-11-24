package org.example.scanner.impl.transformation

import org.example.scanner.dsl.ast.TransformationFunction
import org.example.scanner.metadata.{ColumnMetadata, DateFormatMetadata}
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._
import org.joda.time.{DateTime, Years}
import org.joda.time.format.DateTimeFormat

import scala.util.Try

private[impl] case class Age() extends TransformationFunction {

  override val supportedTypes: Seq[ScannerType] = ScannerTypes.INTEGER_STRING_TYPES :+ DateType
  override val outputScannerType: ScannerType = DecimalType

  private val now: DateTime = DateTime.now()

  private def parseDateTime(datum: Any, formats: Set[String]): Seq[BigDecimal] =
    formats
      .flatMap {
        format =>
          Try {
            val parsedDate = DateTime.parse(datum.toString, DateTimeFormat.forPattern(format))
            BigDecimal(Years.yearsBetween(parsedDate, now).getYears)
          }.toOption
      }
      .toSeq

  override def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Any =
    // All date formats
    datum match {
      case datumAsDate: java.sql.Date =>
        val parsedDate = new DateTime(datumAsDate)
        BigDecimal(Years.yearsBetween(parsedDate, now).getYears)
      case datumAsTimestamp: java.sql.Timestamp =>
        val parsedDate = new DateTime(datumAsTimestamp)
        BigDecimal(Years.yearsBetween(parsedDate, now).getYears)
      case datum => ColumnMetadata
        .findMetadata[DateFormatMetadata]
        .map(metadata => parseDateTime(datum, metadata.dateFormats))
        .getOrElse(Seq.empty[BigDecimal])
    }

}

private[impl] object Age extends KeywordDSL {

  override val keyword: String = "age"

  val ageDSL: FunctionDSL = s"$keyword()"

  def age: Age = Age()
}
