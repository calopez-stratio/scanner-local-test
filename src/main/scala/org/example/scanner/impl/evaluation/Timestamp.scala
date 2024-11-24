package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.utils.FunctionEvaluationUtils.evaluateFunctionConfidence
import org.example.scanner.sdk.ConfidenceEnum._
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.NoParamDSL
import org.example.scanner.sdk.types._

import java.sql

private[impl] case class Timestamp() extends EvaluationFunction {

  override val supportedTypes: Seq[ScannerType] = Seq(StringType, LongType, TimestampType)

  private val TimestampCheck = Timestamp.TimestampCheck
  private val minYearRange = Timestamp.minYearRange
  private val maxYearRange = Timestamp.maxYearRange

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence =
    datum match {
      case _: java.sql.Timestamp => // TimestampType is java.sql.Timestamp at runtime
        HIGH_CONFIDENCE
      case datum: String => evaluateFunctionConfidence(datum, TimestampCheck)
      case datum: Long =>
        if (datum > 0) {
          val datumStr = datum.toString
          if (datumStr.length >= 8 && datumStr.length <= 10) { // Possible Timestamp in seconds, check if 'real' date
            return checkReasonableTimestamp(datum * 1000)
          } else if (datumStr.length > 10 && datumStr.length <= 13) { // Possible Timestamp in milliseconds
            return checkReasonableTimestamp(datum)
          }
        }
        NO_MATCH
      case _ => NO_MATCH

    }

  // Simple algorithm to validate is given Long could be a valid timestamp based on checking the year
  private def checkReasonableTimestamp(ts: Long): Confidence = {
    val year = new sql.Timestamp(ts).toLocalDateTime.getYear
    if (year >= minYearRange)
      if (year <= maxYearRange) MEDIUM_CONFIDENCE // Possible valid ts but not quite sure
      else NO_MATCH // Impossible to validate a ts
    else LOW_CONFIDENCE // Storing a Ts lower than 2000's is rare, but possible
  }

}

private[impl] object Timestamp extends NoParamDSL[Timestamp] {

  override val keyword: String = "timestamp"

  private val TimestampCheckDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^\\d{4}-([0-1][1-2]|[1-9])-(3[0-1]|[1-2][0-9]|0?[1-9])[ ,T]([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9][.]?\\d{1,6}[Z]?([+]\\d{1,2}:\\d{1,2})?$"
          .r
    ),
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^\\d{2,4}[-/]?(3[0-1]|[1-2][0-9]|0?[1-9])[-/]?(3[0-1]|[1-2][0-9]|0?[1-9])[ ,T]\\d{1,2}:\\d{1,2}:\\d{1,2}[.]?\\d{1,6}[Z]?([+]\\d{1,2}:\\d{1,2})?$"
          .r
    ),
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^(3[0-1]|[1-2][0-9]|0?[1-9])[-/]?(3[0-1]|[1-2][0-9]|0?[1-9])[-/]?\\d{2,4}[ ,T]\\d{1,2}:\\d{1,2}:\\d{1,2}[.]?\\d{1,6}[Z]?([+]\\d{1,2}:\\d{1,2})?$"
          .r
    )
  )

  private lazy val TimestampCheck: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(keyword, TimestampCheckDefault)

  private lazy val minYearRange = ScannerConfig.getOrElse(s"$keyword.min_year_range", 2000)
  private lazy val maxYearRange = ScannerConfig.getOrElse(s"$keyword.max_year_range", 2040)
}
