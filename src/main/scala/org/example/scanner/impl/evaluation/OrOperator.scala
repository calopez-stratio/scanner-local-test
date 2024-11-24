package org.example.scanner.impl.evaluation

import org.example.scanner.dsl.ast.LogicOperator
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, DatumFunction}
import org.example.scanner.sdk.traits.impl.KeywordDSL
import org.example.scanner.sdk.types.ScannerType

private[impl] case class OrOperator(lhs: DatumFunction, rhs: DatumFunction) extends LogicOperator {

  override val usesModel: Boolean = lhs.usesModel || rhs.usesModel

  override def evaluateDatum(
                              datum: Any
                            )(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val lhsEvaluation = if (lhs.isCompatible) lhs.evaluateDatum(datum) else NO_MATCH
    if (lhsEvaluation == HIGH_CONFIDENCE) lhsEvaluation
    else Seq(lhsEvaluation, if (rhs.isCompatible) rhs.evaluateDatum(datum) else NO_MATCH).max
  }

  override def isCompatible(implicit datumType: ScannerType): Boolean = lhs.isCompatible || rhs.isCompatible

}

private[impl] object OrOperator extends KeywordDSL {

  override val keyword: String = "|"
}
