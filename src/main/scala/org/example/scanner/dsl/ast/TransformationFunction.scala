package org.example.scanner.dsl.ast

import org.example.scanner.sdk.ConfidenceEnum.Confidence
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, DatumFunction}
import org.example.scanner.sdk.types.ScannerType

private[scanner] trait TransformationFunction extends DatumFunction {

  /**
   * Define a Seq[[ScannerType]] with the supported types your function will return. If not overriden, will support all
   * data types
   *
   * Examples: override val returnDataTypes: Seq[ScannerType] = Seq(StringType, IntegerType, LongType) override val
   * returnDataTypes: Seq[ScannerType] = Seq(BooleanType)
   */

  val outputScannerType: ScannerType

  def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Any

  /**
   * This is the default implementation for [[TransformationFunction]]. These functions shouldn't be using this method
   */
  override def evaluateDatum(
                              datum: Any
                            )(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Confidence = None

  /*  Check if return dataType of the children is supported by the parent && if your children support the types you need
    ...
   */
  def isCompatibleForParentTypes(parentTypes: Seq[ScannerType])(implicit datumType: ScannerType): Boolean =
    isCompatible && parentTypes.contains(outputScannerType)
}
