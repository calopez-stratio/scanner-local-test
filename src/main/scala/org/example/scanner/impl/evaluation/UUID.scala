package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.evaluation.traits.functions.RegexFunction
import org.example.scanner.impl.evaluation.traits.provider.RegexProvider
import org.example.scanner.sdk.ConfidenceEnum.HIGH_CONFIDENCE
import org.example.scanner.sdk.types._

private[impl] case class UUID() extends RegexFunction {
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
}

private[impl] object UUID extends DefaultEnhancedDSL[UUID] with RegexProvider {

  override val keyword: String = "uuid"

  override val enhancedColumnRegexDefault: String = "^.(uu)?id$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

  /**
   * ==Pattern==
   * The hexadecimal digits are grouped as 32 hexadecimal characters with four hyphens:
   * XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX The number of characters per hyphen is 8-4-4-4-12
   *
   * ==Examples==
   * 17100fee-b680-11ed-afa1-0242ac120002, 55d68e5b-ffb5-4026-9386-90a24a75a037
   */
  override val defaultRegexValidations: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$".r
  ))

}
