package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.evaluation.traits.functions.RegexFunction
import org.example.scanner.impl.evaluation.traits.provider.RegexProvider
import org.example.scanner.sdk.ConfidenceEnum.HIGH_CONFIDENCE
import org.example.scanner.sdk.types._

private[impl] case class URI() extends RegexFunction {
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
}

private[impl] object URI extends DefaultEnhancedDSL[URI] with RegexProvider {

  override val keyword: String = "uri"

  override val enhancedColumnRegexDefault: String = "^.*(uri|link|enlace).*$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

  /**
   * ==Pattern==
   * The regex follows the standard RFC3986
   *
   * ==Examples==
   * http://www.example.com/questions/3456/my-document, mailto:John.Doe@example.com,
   * news:comp.infosystems.www.servers.unix, tel:+1-816-555-1212, telnet://192.0.2.16:80/,
   * urn:oasis:names:specification:docbook:dtd:xml:4.1.2, ftp://198.133.219.25:21
   *
   * ==Resources==
   * https://www.rfc-editor.org/rfc/rfc3986, https://snipplr.com/view/6889/regular-expressions-for-uri-validationparsing
   */
  override val defaultRegexValidations: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex =
      "(?i)^([a-z0-9+.-]+):(?://(?:((?:[a-z0-9-._~!$&'()*+,;=:]|%[0-9A-F]{2})*)@)?((?:[a-z0-9-._~!$&'()*+,;=]|%[0-9A-F]{2})*)(?::(\\d*))?(/(?:[a-z0-9-._~!$&'()*+,;=:@/]|%[0-9A-F]{2})*)?|(/?(?:[a-z0-9-._~!$&'()*+,;=:@]|%[0-9A-F]{2})+(?:[a-z0-9-._~!$&'()*+,;=:@/]|%[0-9A-F]{2})*)?)(?:\\?((?:[a-z0-9-._~!$&'()*+,;=:/?@]|%[0-9A-F]{2})*))?(?:#((?:[a-z0-9-._~!$&'()*+,;=:/?@]|%[0-9A-F]{2})*))?$"
        .r
  ))

}
