package org.example.scanner.impl.evaluation

import org.example.scanner.metadata.{ColumnMetadata, DateFormatMetadata}
import org.example.scanner.sdk.ConfidenceEnum._
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.NoParamDSL
import org.example.scanner.sdk.types.ScannerTypes.INTEGER_STRING_TYPES
import org.example.scanner.sdk.types._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try

private[impl] case class Date() extends EvaluationFunction {

  override val supportedTypes: Seq[ScannerType] = INTEGER_STRING_TYPES :+ DateType

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence =
    // All date formats
    datum match {
      case _: java.sql.Date => // DateType is java.sql.Date at runtime
        HIGH_CONFIDENCE
      case datum =>
        val isValidDate = ColumnMetadata
          .findMetadata[DateFormatMetadata]
          .exists(
            _.dateFormats.exists(fmt => Try(DateTime.parse(datum.toString, DateTimeFormat.forPattern(fmt))).isSuccess)
          )
        if (isValidDate) HIGH_CONFIDENCE else NO_MATCH
    }

}

private[impl] object Date extends NoParamDSL[Date] {

  override val keyword: String = "date"

}
