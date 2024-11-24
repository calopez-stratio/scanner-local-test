package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.evaluation.traits.functions.RegexFunction
import org.example.scanner.impl.evaluation.traits.provider.RegexProvider
import org.example.scanner.sdk.ConfidenceEnum.HIGH_CONFIDENCE
import org.example.scanner.sdk.types._

private[impl] case class URL() extends RegexFunction {
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
}

private[impl] object URL extends DefaultEnhancedDSL[URL] with RegexProvider {

  override val keyword: String = "url"

  override val enhancedColumnRegexDefault: String = "^.*(url|link|web|page|enlace).*$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

  /**
   * ==Pattern==
   * The regex will consider a URL valid if it satisfies the following conditions:
   *
   *   - The string can start with either http://, https://, www. or nothing.
   *   - The combined length of the sub-domain and root domain must be between 2 and 256. It should only contain
   *     alphanumeric characters and/or special characters.
   *   - The TLD (Top-Level Domain) should only contain alphabetic characters and it should be between two and six
   *     characters long.
   *   - The end of the URL string could contain alphanumeric characters and/or special characters. And it could repeat
   *     zero or more times.
   *
   * ==Examples==
   * https://www.google.es/, https://es.python.org/, scala-lang.org/
   *
   * ==Resources==
   * https://www.makeuseof.com/regular-expressions-validate-url/
   */
  override val defaultRegexValidations: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex =
      "^(https?://)?(www\\.)?[-a-zA-Z0-9:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b([-a-zA-Z0-9():%_\\+.~#?&/=]*)$".r
  ))
}
