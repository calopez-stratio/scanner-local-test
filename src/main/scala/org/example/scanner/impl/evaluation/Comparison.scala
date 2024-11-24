package org.example.scanner.impl.evaluation

import org.example.scanner.dsl.ast.Comparator.ComparatorType
import org.example.scanner.dsl.ast.{Comparator, TransformationFunction}
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.types._

private[impl] case class Comparison(
                                     lhs: TransformationFunction,
                                     comparator: ComparatorType,
                                     rhs: TransformationFunction
                                   ) extends EvaluationFunction {

  override val supportedTypes: Seq[ScannerType] = ScannerTypes.NUMERIC_STRING_TYPES :+ BooleanType

  override def isCompatible(implicit datumType: ScannerType): Boolean =
    lhs.isCompatibleForParentTypes(supportedTypes) && rhs.isCompatibleForParentTypes(supportedTypes)

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val isMatch = (lhs.evaluate(datum: Any), rhs.evaluate(datum: Any)) match {
      case (lhsEvaluationResult: Seq[_], rhsEvaluationResult: Any) =>
        lhsEvaluationResult.exists(lhsResult => compare(lhsResult, rhsEvaluationResult))
      case (lhsEvaluationResult: Any, rhsEvaluationResult: Seq[_]) =>
        rhsEvaluationResult.exists(rhsResult => compare(lhsEvaluationResult, rhsResult))
      case (lhsEvaluationResult, rhsEvaluationResult) => (null != lhsEvaluationResult && null != rhsEvaluationResult) &&
        (lhsEvaluationResult.getClass == rhsEvaluationResult.getClass) &&
        compare(lhsEvaluationResult, rhsEvaluationResult)
    }
    if (isMatch) HIGH_CONFIDENCE else NO_MATCH
  }

  private def compare(l: Any, r: Any): Boolean =
    (l, r) match {
      case (l: String, r: String) => comparator match {
        case Comparator.eq => l == r
        case _             => false
      }
      case (l: Boolean, r: Boolean) => comparator match {
        case Comparator.eq => l == r
        case _             => false
      }
      case (l: BigDecimal, r: BigDecimal) => comparator match {
        case Comparator.eq  => l == r
        case Comparator.gte => l >= r
        case Comparator.lte => l <= r
        case Comparator.gt  => l > r
        case Comparator.lt  => l < r
        case _              => false
      }
      case _ => false
    }

}
