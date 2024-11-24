package org.example.scanner.impl.transformation

import org.example.scanner.dsl.ast.TransformationFunction
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._

private[impl] case class Trim(func: TransformationFunction) extends TransformationFunction {

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
  override val outputScannerType: ScannerType = StringType

  override def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Any =
    func.evaluate(datum: Any).toString.trim

}

private[impl] object Trim extends KeywordDSL {

  override val keyword: String = "trim"

  def trimDSL(func: FunctionDSL): FunctionDSL = s"$keyword($func)"

  def trim(func: TransformationFunction): Trim = Trim(func)
}
