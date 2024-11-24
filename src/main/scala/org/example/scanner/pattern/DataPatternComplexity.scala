package org.example.scanner.pattern

import org.example.scanner.impl.utils.NumberUtils.BigDecimalImprovements

object DataPatternComplexity {

  type DSLComplexity = Double

  // account_number();col_name_rlike("^(?!.*balance.*)(checking|savings|bank|debit|fund|holder|swift)?"$)
  val NoParamFunctionWithColname: DSLComplexity = 0.05d
  // account_number(), ..., country(), ..., birth_date(), email(), imei(), ...
  val NoParamFunction: DSLComplexity = 0.1d
  // account_number("USA"), passport_number("ESP"), ...
  val CountryCodeFunctionWithParam: DSLComplexity = 0.0d
  // in("x", "y"), ...
  val InFunction: DSLComplexity = 0.4d
  // rlike(".*")
  val RlikeFunction: DSLComplexity = 0.5d
  // distribution
  val NumericFunction: DSLComplexity = 0.6d
  // lower(), upper(), length(), ...
  val NoPriority: DSLComplexity = 1.0d

  // To create complexity based on multiple inputs
  def functionCombination(values: Seq[Double]): DSLComplexity = {
    assert(values.nonEmpty, "Function combination complexity cannot be empty")
    BigDecimal(values.sum / values.size).roundToDouble(2)
  }

}
