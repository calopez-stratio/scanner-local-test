package org.example.scanner.impl.transformation

import org.example.scanner.dsl.ast.TransformationFunction
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._

private[impl] case class Length(func: TransformationFunction) extends TransformationFunction {

  // TODO: De momento solo se aceptan strings porque los col siempre se van a parsear a Double
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
  override val outputScannerType: ScannerType = DecimalType

  override def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Any = {
    val valueEv = func.evaluate(datum: Any)
    BigDecimal(valueEv.toString.length)
  }

}

private[impl] object Length extends KeywordDSL {

  override val keyword: String = "length"

  def lengthDSL(func: FunctionDSL): FunctionDSL = s"$keyword($func)"

  def length(func: TransformationFunction): Length = Length(func)

}
