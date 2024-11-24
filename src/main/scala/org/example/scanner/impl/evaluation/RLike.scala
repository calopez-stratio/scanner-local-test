package org.example.scanner.impl.evaluation

import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types.ScannerType

private[impl] case class RLike(regex: String) extends EvaluationFunction {

  private val compiledRegex = regex.r

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val isMatch = Option(datum).exists(datum => compiledRegex.findFirstIn(datum.toString).isDefined)
    if (isMatch) HIGH_CONFIDENCE else NO_MATCH
  }

}

private[impl] object RLike extends KeywordDSL {

  override val keyword: String = "rlike"

  def stringDSL(string: String): FunctionDSL = s"""$keyword("$string")"""

}
