package org.example.scanner.impl.evaluation

import org.example.scanner.dsl.ast.DatumAndColumnFunction
import org.example.scanner.impl.column.ColNameRLikeMode
import org.example.scanner.impl.column.ColNameRLikeMode.ColNameRLikeModeType
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.{dslfunctions, functions}
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.sdk.traits.impl.FunctionDSL

private[impl] object BirthDate extends DefaultEnhancedDSL[Between] {

  override val keyword: String = "birth_date"

  val enhancedColumnRegexDefault: String = "^(.*birth.*date.*|.*dob.*|.*natal.*|.*fec.*nacim.*|.*fecha.*nacimiento.*)$"

  override lazy val colNameMode: ColNameRLikeModeType = ScannerConfig
    .getColNameModeOrElse(s"$keyword.col_name_mode", ColNameRLikeMode.strict)

  private lazy val minAge = ScannerConfig.getOrElse(s"$keyword.min_age", 18)
  private lazy val maxAge = ScannerConfig.getOrElse(s"$keyword.max_age", 120)

  override lazy val enhancedDSL: FunctionDSL = dslfunctions.datumAndColumnFunction(
    dslfunctions.between(dslfunctions.age, minAge, maxAge),
    dslfunctions.colNameRLike(enhancedColumnRegex, colNameMode)
  )

  override lazy val enhancedFunction: DatumAndColumnFunction = functions.datumAndColumnFunction(
    functions.between(functions.age, minAge, maxAge),
    functions.colNameRLike(enhancedColumnRegex, colNameMode)
  )

  // Not used
  override def apply(): Between = null
}
