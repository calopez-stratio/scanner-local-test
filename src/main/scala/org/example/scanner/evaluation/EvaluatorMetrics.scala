package org.example.scanner.evaluation

import org.example.scanner.evaluation.ColumnPatternMatch.{METRIC_COUNT_NAME, METRIC_NULLS_NAME}
import org.example.scanner.impl.utils.NumberUtils.BigDecimalImprovements
import org.example.scanner.pattern.DataPattern
import org.example.scanner.sdk.ConfidenceEnum.{ConfidenceType, HIGH, IGNORE, LOW, MEDIUM, VERY_HIGH, VERY_LOW}

case class EvaluatorMetrics(
                             dataPattern: DataPattern,
                             columnName: String,
                             confidenceLevel: String,
                             confidence: Double,
                             precision: Double,
                             percentageMatches: Double,
                             numMatches: Int,
                             numNulls: Int,
                             numRows: Int
                           )

object EvaluatorMetrics {
  private val HIGH_CONFIDENCE_MULTIPLIER: Double = 1.00
  private val MEDIUM_CONFIDENCE_MULTIPLIER: Double = 0.66
  private val LOW_CONFIDENCE_MULTIPLIER: Double = 0.33

  private val VERY_HIGH_CONFIDENCE_THRESHOLD: Double = 80
  private val HIGH_CONFIDENCE_THRESHOLD: Double = 60
  private val MEDIUM_CONFIDENCE_THRESHOLD: Double = 40
  private val LOW_CONFIDENCE_THRESHOLD: Double = 20

  def apply(dataPattern: DataPattern, columnName: String): EvaluatorMetrics =
    EvaluatorMetrics(dataPattern, columnName, "very_high", 100.0, 100.0, 100.0, 100, 0, 100)

  val colNameRLikeDefaultConfidence: Option[Map[ConfidenceType, Int]] = Some(Map(HIGH -> 0))

  def getConfidenceValue(totalConfidence: Double): ConfidenceType =
    totalConfidence match {
      case x if x >= VERY_HIGH_CONFIDENCE_THRESHOLD => VERY_HIGH
      case x if x >= HIGH_CONFIDENCE_THRESHOLD      => HIGH
      case x if x >= MEDIUM_CONFIDENCE_THRESHOLD    => MEDIUM
      case x if x >= LOW_CONFIDENCE_THRESHOLD       => LOW
      case _                                        => VERY_LOW
    }
  private[scanner] def calculateConfidence(
                                            confidences: Map[ConfidenceType, Int],
                                            totalElements: Int
                                          ): (ConfidenceType, Double) = {
    val totalConfidence =
      if (colNameRLikeDefaultConfidence.exists(confidences.equals)) 100
      else if (totalElements == 0) 0
      else {
        val confidenceList = confidences.map {
          case (confidenceValue, numMatches) => confidenceValue match {
            case HIGH   => numMatches * HIGH_CONFIDENCE_MULTIPLIER
            case MEDIUM => numMatches * MEDIUM_CONFIDENCE_MULTIPLIER
            case LOW    => numMatches * LOW_CONFIDENCE_MULTIPLIER
            case IGNORE => 0
          }
        }
        BigDecimal(confidenceList.sum * 100 / totalElements).roundToDouble(2)
      }
    val newConfidenceValue = getConfidenceValue(totalConfidence)
    (newConfidenceValue, totalConfidence)
  }

  def calculatePatternPrecision(
                                 dataPattern: DataPattern,
                                 columnPatternMatches: Seq[ColumnPatternMatch],
                                 positiveColumns: Seq[String]
                               ): Double = {
    val foundPositives = columnPatternMatches
      .filter(columnPatternMatch => positiveColumns.contains(columnPatternMatch.colName))
      .flatMap(_.patternMatches)
      .filter(_.dataPattern.equals(dataPattern))
      .flatMap(x => x.confidences.filterNot { case (confidence, _) => confidence == IGNORE }.values)
      .sum
    val falsePositives = columnPatternMatches
      .filter(columnPatternMatch => !positiveColumns.contains(columnPatternMatch.colName))
      .flatMap(_.patternMatches)
      .filter(_.dataPattern.equals(dataPattern))
      .flatMap(x => x.confidences.filterNot { case (confidence, _) => confidence == IGNORE }.values)
      .sum
    val precision = (foundPositives.toDouble * 100) / (foundPositives + falsePositives)
    BigDecimal(precision).roundToDouble(2)
  }

  def getEvaluatorMetrics(
                           columnPatternMatches: Seq[ColumnPatternMatch],
                           targetColumns: Option[Seq[String]] = None
                         ): Seq[EvaluatorMetrics] = {
    val filteredColumnPatternMatches = columnPatternMatches.map(
      columnPatternMatch =>
        columnPatternMatch
          .copy(patternMatches = columnPatternMatch.patternMatches.filterNot(_.confidences.keySet == Set(IGNORE)))
    )
    filteredColumnPatternMatches.flatMap {
      columnPatternMatch =>
        columnPatternMatch
          .patternMatches
          .map {
            patternMatch =>
              val totalElements = columnPatternMatch.columnMetrics(METRIC_COUNT_NAME)
              val ignoredElements = patternMatch.confidences.getOrElse(IGNORE, 0)
              val totalProcessedElements = totalElements - ignoredElements
              val percentageMatches =
                if (totalProcessedElements > 0) (patternMatch.confidences.values.sum.toDouble - ignoredElements) /
                  totalProcessedElements
                else 0
              val (level, newConfidence) = calculateConfidence(patternMatch.confidences, totalProcessedElements)
              val precision = calculatePatternPrecision(
                patternMatch.dataPattern,
                filteredColumnPatternMatches,
                targetColumns.getOrElse(Seq(columnPatternMatch.colName))
              )
              EvaluatorMetrics(
                dataPattern = patternMatch.dataPattern,
                columnName = columnPatternMatch.colName,
                confidenceLevel = level.toString,
                confidence = newConfidence,
                percentageMatches = BigDecimal(percentageMatches * 100).roundToDouble(2),
                precision = precision,
                numMatches = patternMatch.confidences.values.sum - ignoredElements,
                numNulls = columnPatternMatch.columnMetrics(METRIC_NULLS_NAME),
                numRows = totalElements + columnPatternMatch.columnMetrics(METRIC_NULLS_NAME) - ignoredElements
              )
          }
    }
  }
}
