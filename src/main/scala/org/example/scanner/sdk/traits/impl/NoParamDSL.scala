package org.example.scanner.sdk.traits.impl

import org.example.scanner.sdk.traits.dsl.DataPatternExpression

trait NoParamDSL[T <: DataPatternExpression] extends KeywordDSL {

  self: KeywordDSL =>

  def apply(): T

  def noParamDSL: FunctionDSL = s"$keyword()"

  def noParamFunction: T = self.apply()
}
