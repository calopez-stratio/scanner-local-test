package org.example.scanner.impl.evaluation.traits.provider

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.sdk.traits.impl.KeywordDSL

private[impl] trait RegexProvider extends Provider {

  self: KeywordDSL =>

  val defaultRegexValidations: List[RegexValidation]

  override def getRegexValidations: List[RegexValidation] =
    ScannerConfig.getRegexValidationOrElse(keyword, defaultRegexValidations)

}
