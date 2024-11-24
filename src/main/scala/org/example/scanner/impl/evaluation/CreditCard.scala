package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.evaluation.traits.functions.RegexFunction
import org.example.scanner.impl.evaluation.traits.provider.RegexProvider
import org.example.scanner.impl.utils.Checksums
import org.example.scanner.sdk.ConfidenceEnum.HIGH_CONFIDENCE
import org.example.scanner.sdk.types.{ScannerType, ScannerTypes}

private[impl] case class CreditCard() extends RegexFunction {
  override val supportedTypes: Seq[ScannerType] = ScannerTypes.INTEGER_STRING_TYPES
}

private[impl] object CreditCard extends DefaultEnhancedDSL[CreditCard] with RegexProvider {

  override val keyword: String = "credit_card"

  override val enhancedColumnRegexDefault: String =
    "^.*(credit|debit)?.*(card|carte|carta|cartão|cartao|karte|karta|tarjeta|visa|amex).*(number|no|num|n|numero|#)?.*$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

  override val defaultRegexValidations: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex =
    // Matches Visa, MasterCard, American Express, Diners Club, Discover and JCB cards
      "^(?>4[0-9]{3}([- ]?[0-9]{4}){3}|[25][1-7][0-9]{2}([- ]?[0-9]{4}){3}|6(?>011|5[0-9]{2})([- ]?[0-9]{4}){3}|3[47][0-9]{2}([- ]?[0-9]{4}){3}|3(?>0[0-5]|[68][0-9])[0-9]([- ]?[0-9]{4}){3}|(?>2131|1800|35[0-9]{2})([- ]?[0-9]{4}){3})$"
        .r,
    checksumCleanChars = List('-', ' '),
    checksumFunction = Checksums.luhnChecksum
  ))

}
