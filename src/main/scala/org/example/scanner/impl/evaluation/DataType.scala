package org.example.scanner.impl.evaluation

import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types.{NumericType, ScannerType, ScannerTypes}

private[impl] case class DataType(dataType: ScannerType) extends EvaluationFunction {

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence =
    if (dataType == NumericType && ScannerTypes.NUMERIC_TYPES.contains(scannerType)) HIGH_CONFIDENCE
    else if (dataType == scannerType) HIGH_CONFIDENCE
    else NO_MATCH

}

private[impl] object DataType extends KeywordDSL {

  override val keyword: String = "data_type"

  def dataTypeDSL(dataType: ScannerType): FunctionDSL = s"$keyword(${s""""${dataType.name}""""})"

}
