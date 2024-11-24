package org.example.scanner.pattern

import org.example.scanner.dsl.DSLParser
import org.example.scanner.dsl.ast.{ColumnFunction, DatumAndColumnFunction}
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, NO_MATCH}
import org.example.scanner.sdk.traits.dsl
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, DatumFunction}
import org.example.scanner.sdk.types.ScannerType

case class DataPatternExpression(patternDSL: String) {

  private lazy val dataPatternExpression: dsl.DataPatternExpression = DSLParser.parseDataPatternExpression(patternDSL)

  lazy val maybeDatumFunction: Option[DatumFunction] = dataPatternExpression match {
    case datumFunction: DatumFunction             => Some(datumFunction)
    case DatumAndColumnFunction(datumFunction, _) => Some(datumFunction)
    case _                                        => None
  }
  lazy val maybeColumnFunction: Option[ColumnFunction] = dataPatternExpression match {
    case DatumAndColumnFunction(_, columnFunction) => Some(columnFunction)
    case _                                         => None
  }
  lazy val hasDatumFunction: Boolean = dataPatternExpression match {
    case DatumAndColumnFunction(null, _) => false
    case _                               => true
  }
  lazy val usesModel: Boolean = dataPatternExpression match {
    case datumFunction: DatumFunction             => Option(datumFunction.usesModel).getOrElse(false)
    case DatumAndColumnFunction(null, _)          => false
    case DatumAndColumnFunction(datumFunction, _) => Option(datumFunction.usesModel).getOrElse(false)
    case _                                        => false
  }

  def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence =
    dataPatternExpression match {
      case datumFunction: DatumFunction =>
        if (datumFunction.isCompatible) datumFunction.evaluateDatum(datum) else NO_MATCH
      case DatumAndColumnFunction(null, _) => NO_MATCH
      case DatumAndColumnFunction(datumFunction, _) =>
        if (datumFunction.isCompatible) datumFunction.evaluateDatum(datum) else NO_MATCH
    }

}
