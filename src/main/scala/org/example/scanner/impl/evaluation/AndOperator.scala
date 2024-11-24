package org.example.scanner.impl.evaluation

import org.example.scanner.dsl.ast.LogicOperator
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, DatumFunction}
import org.example.scanner.sdk.traits.impl.KeywordDSL
import org.example.scanner.sdk.types.ScannerType

private[impl] case class AndOperator(lhs: DatumFunction, rhs: DatumFunction) extends LogicOperator {
  override val usesModel: Boolean = lhs.usesModel || rhs.usesModel

  // First evaluateDataPatterns lhs and check if given any result to continue with rhs
  override def evaluateDatum(
                              datum: Any
                            )(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val lhsEvaluation = if (lhs.isCompatible) lhs.evaluateDatum(datum) else NO_MATCH
    if (lhsEvaluation.isDefined) Seq(lhsEvaluation, if (rhs.isCompatible) rhs.evaluateDatum(datum) else NO_MATCH).min
    else NO_MATCH
  }

  override def isCompatible(implicit datumType: ScannerType): Boolean = lhs.isCompatible || rhs.isCompatible

}

private[impl] object AndOperator extends KeywordDSL {

  override val keyword: String = "&"
}
