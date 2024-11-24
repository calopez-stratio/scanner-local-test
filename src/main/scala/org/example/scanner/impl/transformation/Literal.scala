package org.example.scanner.impl.transformation

import org.example.scanner.dsl.ast.TransformationFunction
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.example.scanner.sdk.types.{ScannerType, ScannerTypes}

private[impl] case class Literal(value: Any) extends TransformationFunction {

  override val outputScannerType: ScannerType = ScannerTypes.getScannerTypeFromAny(value)

  override def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Any = value

}
