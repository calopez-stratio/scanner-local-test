package org.example.scanner.impl.column

import org.example.scanner.dsl.ast.ColumnFunction
import org.example.scanner.evaluation.EvaluatorMetrics
import org.example.scanner.impl.column.ColNameRLikeMode.ColNameRLikeModeType
import org.example.scanner.sdk.ConfidenceEnum.{ConfidenceType, HIGH, IGNORE, LOW, findValueById}
import org.example.scanner.sdk.traits.impl.KeywordDSL

import scala.collection.mutable

private[scanner] case class ColNameRLike(regex: String, mode: ColNameRLikeModeType) extends ColumnFunction {

  private def calculateConfidences(
                                    isMatch: Boolean,
                                    confidences: Map[ConfidenceType, Int],
                                    restrictive: Boolean
                                  ): Map[ConfidenceType, Int] = {
    val newConfidences = mutable.Map.empty[ConfidenceType, Int]
    if (isMatch) {
      confidences.foreach {
        case (confidence, matches) =>
          val newId = math.min(confidence.id + 1, HIGH.id)
          val newConfidence = findValueById(newId).get
          newConfidences(newConfidence) = matches + newConfidences.getOrElse(newConfidence, 0)
      }
      newConfidences.toMap
    } else if (restrictive) {
      confidences.foreach {
        case (confidence, matches) =>
          val newId = math.max(confidence.id - 1, LOW.id)
          val newConfidence = findValueById(newId).get
          newConfidences(newConfidence) = matches + newConfidences.getOrElse(newConfidence, 0)
      }
      newConfidences.toMap
    } else confidences

  }

  override def evaluate(
                         colName: String,
                         maybeConfidences: Option[Map[ConfidenceType, Int]],
                         hasColumnFunction: Boolean
                       ): Option[Map[ConfidenceType, Int]] = {
    val isMatch = regex
      .replaceAllLiterally("\\\\", "\\")
      .replaceAllLiterally("\\\"", "\"")
      .r
      .findFirstIn(colName.toLowerCase())
      .isDefined

    val amountIgnored = maybeConfidences.flatMap(_.get(IGNORE))
    maybeConfidences
      .map(_.filterKeys(key => key != IGNORE))
      .flatMap {
        confidences =>
          val newConfidence = mode match {
            case ColNameRLikeMode.`max` => // - Maximizes confidence if matchs
              if (isMatch) Map(HIGH -> confidences.values.sum) else confidences
            case ColNameRLikeMode.`sum` => // - Adds 1 to confidence if matchs
              calculateConfidences(isMatch, confidences, restrictive = false)
            case ColNameRLikeMode.soft_restrictive => // - Adds 1 or subtracts 1 to confidence
              calculateConfidences(isMatch, confidences, restrictive = true)
            case ColNameRLikeMode.hard_restrictive => // - Maximizes or minimizes the confidence
              if (isMatch) Map(HIGH -> confidences.values.sum) else Map(LOW -> confidences.values.sum)
            case ColNameRLikeMode.strict => // - Adds 1 to confidence or no match
              if (isMatch) calculateConfidences(isMatch, confidences, restrictive = false) else null
            case ColNameRLikeMode.check => // - Same confidence or no match
              if (isMatch) confidences else null
          }
          Option(newConfidence)
      }
      .map(confidences => confidences ++ amountIgnored.map(amount => Map(IGNORE -> amount)).getOrElse(Map.empty))
      .orElse(if (isMatch && !hasColumnFunction) EvaluatorMetrics.colNameRLikeDefaultConfidence else None)
  }

}

private[scanner] object ColNameRLike extends KeywordDSL {

  override val keyword: String = "col_name_rlike"

  def apply(regex: String): ColNameRLike = ColNameRLike(regex, ColNameRLikeMode.sum)

  def apply(regex: String, modeStr: String): ColNameRLike =
    ColNameRLikeMode.findValue(modeStr).map(mode => ColNameRLike(regex, mode)).getOrElse(apply(regex))

  def colNameRLike(regex: String): String = s"""$keyword("$regex")"""

  def colNameRLike(regex: String, mode: ColNameRLikeModeType): String = s"""$keyword("$regex", $mode)"""

}

