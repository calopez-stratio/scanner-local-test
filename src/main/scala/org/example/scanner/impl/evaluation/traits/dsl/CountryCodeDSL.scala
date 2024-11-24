package org.example.scanner.impl.evaluation.traits.dsl

import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.sdk.traits.dsl.EvaluationFunction
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}

private[impl] trait CountryCodeDSL[T <: EvaluationFunction] extends DefaultEnhancedDSL[T] {

  self: KeywordDSL =>

  def apply(countryCode: Option[CountryCodeType]): T

  def apply(): T = self.apply(None)

  override def noParamFunction: T = self.apply()

  def countryCodeDSL(countryCode: CountryCodeType): FunctionDSL = s"""$keyword("$countryCode")"""

  def countryCodeFunction(countryCode: Option[CountryCodeType]): T = self.apply(countryCode)

}
