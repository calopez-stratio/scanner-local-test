package org.example.scanner.impl.evaluation

import org.example.scanner.dsl.ast.TransformationFunction
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types.{ScannerType, ScannerTypes}

private[impl] case class Between(func: TransformationFunction, minValue: Double, maxValue: Double)
  extends EvaluationFunction {
  override val supportedTypes: Seq[ScannerType] = ScannerTypes.NUMERIC_STRING_TYPES

  override def isCompatible(implicit datumType: ScannerType): Boolean = func.isCompatibleForParentTypes(supportedTypes)

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val isMatch = func.evaluate(datum: Any) match {
      case evaluationResult: Seq[_] => evaluationResult.asInstanceOf[Seq[BigDecimal]].exists(result => between(result))
      case evaluationResult: BigDecimal => between(evaluationResult)
      case _                            => false
    }
    if (isMatch) HIGH_CONFIDENCE else NO_MATCH
  }

  private def between(value: BigDecimal): Boolean = value >= minValue && value <= maxValue

}

private[impl] object Between extends KeywordDSL {

  override val keyword: String = "between"

  def betweenDSL(func: FunctionDSL, min: Double, max: Double): FunctionDSL = s"$keyword($func, $min, $max)"

  def between(func: TransformationFunction, min: Double, max: Double): Between = Between(func, min, max)

}
