package org.example.scanner.impl.transformation

import org.example.scanner.dsl.ast.TransformationFunction
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._

private[impl] case class Upper(func: TransformationFunction) extends TransformationFunction {

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
  override val outputScannerType: ScannerType = StringType

  override def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Any =
    func.evaluate(datum: Any).toString.toUpperCase

}

private[impl] object Upper extends KeywordDSL {

  override val keyword: String = "upper"

  def upperDSL(func: FunctionDSL): FunctionDSL = s"$keyword($func)"

  def upper(func: TransformationFunction): Upper = Upper(func)
}
