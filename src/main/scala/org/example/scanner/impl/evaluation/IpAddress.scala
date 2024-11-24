package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.evaluation.traits.functions.RegexFunction
import org.example.scanner.impl.evaluation.traits.provider.RegexProvider
import org.example.scanner.sdk.ConfidenceEnum.HIGH_CONFIDENCE
import org.example.scanner.sdk.types._

private[impl] case class IpAddress() extends RegexFunction {
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
}

private[impl] object IpAddress extends DefaultEnhancedDSL[IpAddress] with RegexProvider {

  override val keyword: String = "ip_address"

  override val enhancedColumnRegexDefault: String =
    "^(direccion[-._ /+:;,*<>&]*)?(ip|network|internet[-._ /+:;,*<>&]*?protocol)(v4|v6)?[-._ /+:;,*<>&]*?(address|number|direction)?$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

  override val defaultRegexValidations: List[RegexValidation] = List(
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^((?>([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$"
          .r
    ),
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|::(ffff(:0{1,4})?:)?)$"
          .r
    )
  )

}
