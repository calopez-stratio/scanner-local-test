package org.example.scanner.impl.transformation

import org.example.scanner.dsl.ast.TransformationFunction
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._

private[impl] case class Replace(func: TransformationFunction, regex: String, replacement: String)
  extends TransformationFunction {

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
  override val outputScannerType: ScannerType = StringType

  override def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Any =
    func.evaluate(datum: Any).toString.replaceAll(regex, replacement)

}

private[impl] object Replace extends KeywordDSL {

  override val keyword: String = "replace"

  def replaceDSL(func: FunctionDSL, regex: String, replacement: String): FunctionDSL =
    s"""$keyword($func, "$regex", "$replacement")"""

  def replace(func: TransformationFunction, regex: String, replacement: String): Replace =
    Replace(func, regex, replacement)

}
