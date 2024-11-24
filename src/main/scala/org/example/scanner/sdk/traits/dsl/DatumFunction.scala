package org.example.scanner.sdk.traits.dsl

import org.example.scanner.sdk.ConfidenceEnum.Confidence
import org.example.scanner.sdk.types.ScannerType

private[scanner] trait DatumFunction extends DataPatternExpression {

  /** Determines if this implementation uses a model-base evaluation, default false */
  val usesModel: Boolean = false

  /**
   * Define a Seq[[ScannerType]] with the supported types your function can handle. If not overriden, will support all
   * data types
   *
   * Examples: override val supportedDataTypes: Seq[ScannerType] = Seq(StringType, IntegerType, LongType) override val
   * supportedDataTypes: Seq[ScannerType] = Seq(BooleanType)
   */
  val supportedTypes: Seq[ScannerType] = null

  /** Evaluates a given datum to return a [[Confidence]]. This is basically the function implementation. */
  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence

  /**
   * Default implementation to determine is a given datum is compatible and should be evaluated. This implementation
   * should be rarely overriden. If your function needs to support all data types, the value [[supportedTypes]] must not
   * be overriden.
   */
  def isCompatible(implicit datumType: ScannerType): Boolean = Option(supportedTypes).forall(_.contains(datumType))
}