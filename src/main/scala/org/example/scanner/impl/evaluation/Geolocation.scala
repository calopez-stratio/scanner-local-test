package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.evaluation.traits.functions.RegexFunction
import org.example.scanner.impl.evaluation.traits.provider.RegexProvider
import org.example.scanner.sdk.ConfidenceEnum.HIGH_CONFIDENCE
import org.example.scanner.sdk.types._

private[impl] case class Geolocation() extends RegexFunction {
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
}

private[impl] object Geolocation extends DefaultEnhancedDSL[Geolocation] with RegexProvider {

  override val keyword: String = "geolocation"

  override val enhancedColumnRegexDefault: String =
    "^.*(geo(loc)?|coordinates|coord|lon(gitude)?|lat(itude)?|viewpoint).*(position)?.*$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"${Geolocation.keyword}.col_name_regex", enhancedColumnRegexDefault)

  override val defaultRegexValidations: List[RegexValidation] = List(
    // Geolocation Formatted Both
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "[-+]?(?>(90º ?0' ?(0(\\.0+)?\")?)|([1-8]?[0-9]º ?[1-5]?[0-9]' ?([1-5]?[0-9](\\.[0-9]+)?\")?)) ?, ?[-+]?(?>(180º ?0' ?(0(\\.0+)?\")?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))º ?[1-5]?[0-9]'( ?[1-5]?[0-9](\\.[0-9]+)?\")?))$"
          .r
    ),
    // Geolocation Formatted Both Parenthesis
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^\\([-+]?(?>(90º ?0' ?(0(\\.0+)?\")?)|([1-8]?[0-9]º ?[1-5]?[0-9]' ?([1-5]?[0-9](\\.[0-9]+)?\")?)) ?, ?[-+]?(?>(180º ?0' ?(0(\\.0+)?\")?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))º ?[1-5]?[0-9]'( ?[1-5]?[0-9](\\.[0-9]+)?\")?))\\)$"
          .r
    ),
    // Geolocation FormattedBothBrackets
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^\\[[-+]?(?>(90º ?0' ?(0(\\.0+)?\")?)|([1-8]?[0-9]º ?[1-5]?[0-9]' ?([1-5]?[0-9](\\.[0-9]+)?\")?)) ?, ?[-+]?(?>(180º ?0' ?(0(\\.0+)?\")?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))º ?[1-5]?[0-9]'( ?[1-5]?[0-9](\\.[0-9]+)?\")?))\\]$"
          .r
    ),
    // Geolocation Formatted Lat Or Lon Single
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^[-+]?(?>(180º ?0' ?(0(\\.0+)?\")?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))º ?[1-5]?[0-9]'( ?[1-5]?[0-9](\\.[0-9]+)?\")?))$"
          .r
    ),
    // Geolocation Numeric Both
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^[-+]?(?>(90\\.0+)|([1-8]?[0-9]\\.[0-9]+)) ?, ?[-+]?(?>(180(\\.0+)?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))\\.[0-9]+))$"
          .r
    ),
    // Geolocation Numeric Both Parenthesis
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^\\([-+]?(?>(90\\.0+)|([1-8]?[0-9]\\.[0-9]+)) ?, ?[-+]?(?>(180(\\.0+)?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))\\.[0-9]+))\\)$"
          .r
    ),
    // Geolocation Numeric Both Brackets
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^\\[[-+]?(?>(90\\.0+)|([1-8]?[0-9]\\.[0-9]+)) ?, ?[-+]?(?>(180(\\.0+)?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))\\.[0-9]+))\\]$"
          .r
    ),
    // Geolocation JsonRegexNu
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^\\{[ \\n]*\"[a-zA-Z]+\": *-?(?>(90\\.0+)|([1-8]?[0-9]\\.[0-9]+)),[ \\n]*\"[a-zA-Z]+\": *-?(?>(180(\\.0+)?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))\\.[0-9]+))[ \\n]*\\}$"
          .r
    ),
    // Geolocation JsonRegex Numeric
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^\\{[ \\n]*\"[a-zA-Z]+\": *\"-?(?>(90\\.0+)|([1-8]?[0-9]\\.[0-9]+))\",[ \\n]*\"[a-zA-Z]+\": *\"-?(?>(180(\\.0+)?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))\\.[0-9]+))\"[ \\n]*\\}$"
          .r
    ),
    // Geolocation JsonRegex Form
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^\\{[ \\n]*\"[a-zA-Z]+\": *\"[-+]?(?>(90º ?0' ?(0(\\.0+)?\")?)|([1-8]?[0-9]º ?[1-5]?[0-9]' ?([1-5]?[0-9](\\.[0-9]+)?\")?))\",[ \\n]*\"[a-zA-Z]+\": *\"[-+]?(?>(180º ?0' ?(0(\\.0+)?\")?)|((?>(1[0-7][0-9])|([1-9]?[0-9]))º ?[1-5]?[0-9]'( ?[1-5]?[0-9](\\.[0-9]+)?\")?))\"[ \\n]*\\}$"
          .r
    )
  )

}
