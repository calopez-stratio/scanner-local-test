package org.example.scanner.impl.evaluation.traits.dsl

import org.example.scanner.dsl.ast.DatumAndColumnFunction
import org.example.scanner.impl.column.ColNameRLikeMode
import org.example.scanner.impl.column.ColNameRLikeMode.ColNameRLikeModeType
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.{dslfunctions,functions}
import org.example.scanner.sdk.traits.dsl.EvaluationFunction
import org.example.scanner.sdk.traits.impl.{FunctionDSL, NoParamDSL}

private[impl] trait DefaultEnhancedDSL[T <: EvaluationFunction] extends NoParamDSL[T] {

  val enhancedColumnRegexDefault: String

  lazy val enhancedColumnRegex: FunctionDSL = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

  lazy val colNameMode: ColNameRLikeModeType = ScannerConfig
    .getColNameModeOrElse(s"$keyword.col_name_mode", ColNameRLikeMode.sum)

  lazy val enhancedDSL: FunctionDSL = dslfunctions
    .datumAndColumnFunction(noParamDSL, dslfunctions.colNameRLike(enhancedColumnRegex, colNameMode))

  lazy val enhancedFunction: DatumAndColumnFunction = functions
    .datumAndColumnFunction(noParamFunction, functions.colNameRLike(enhancedColumnRegex, colNameMode))
}
