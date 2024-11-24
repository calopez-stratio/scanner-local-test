package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.evaluation.traits.functions.RegexFunction
import org.example.scanner.impl.evaluation.traits.provider.RegexProvider
import org.example.scanner.impl.utils.Checksums
import org.example.scanner.sdk.ConfidenceEnum.HIGH_CONFIDENCE
import org.example.scanner.sdk.types.{ScannerType, ScannerTypes}

private[impl] case class IMEI() extends RegexFunction {
  override val supportedTypes: Seq[ScannerType] = ScannerTypes.INTEGER_STRING_TYPES
}

private[impl] object IMEI extends DefaultEnhancedDSL[IMEI] with RegexProvider {

  override val keyword: String = "imei"

  override val enhancedColumnRegexDefault: String = "^.*imei.*(number|no|num|n)?.*$"

  override val defaultRegexValidations: List[RegexValidation] = List(
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = "^[0-9]{15}$".r, checksumFunction = Checksums.luhnChecksum)
  )

}
