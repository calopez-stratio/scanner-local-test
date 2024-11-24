package org.example.scanner.impl.evaluation

import org.example.scanner.sdk.ConfidenceEnum
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, DatumFunction}
import org.example.scanner.sdk.traits.impl.KeywordDSL
import org.example.scanner.sdk.types.ScannerType

private[impl] case class NotOperator(function: DatumFunction) extends DatumFunction {

  override val usesModel: Boolean = function.usesModel

  override def evaluateDatum(
                              datum: Any
                            )(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence =
    if (!function.isCompatible || function.evaluateDatum(datum) == NO_MATCH) ConfidenceEnum.HIGH_CONFIDENCE
    else NO_MATCH

  override def isCompatible(implicit datumType: ScannerType): Boolean = true
}

private[impl] object NotOperator extends KeywordDSL {

  override val keyword: String = "!"
}
