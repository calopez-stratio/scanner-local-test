package org.example.scanner.impl.evaluation.traits.provider

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.CountryCode
import org.example.scanner.sdk.traits.impl.KeywordDSL

import scala.util.Random

private[impl] trait CountryCodeProvider extends RegexProvider {
  self: KeywordDSL =>
  val chileRegexValidationsDefault: List[RegexValidation]
  val ukRegexValidationsDefault: List[RegexValidation]
  val canadaRegexValidationsDefault: List[RegexValidation]
  val usaRegexValidationsDefault: List[RegexValidation]
  val ecuadorRegexValidationsDefault: List[RegexValidation]
  val mexicoRegexValidationsDefault: List[RegexValidation]
  val spainRegexValidationsDefault: List[RegexValidation]
  val franceRegexValidationsDefault: List[RegexValidation]

  lazy val spainRegexValidations: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(s"$keyword.${CountryCode.spain}", spainRegexValidationsDefault)

  lazy val franceRegexValidations: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(s"$keyword.${CountryCode.france}", franceRegexValidationsDefault)

  lazy val ukRegexValidations: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(s"$keyword.${CountryCode.uk}", ukRegexValidationsDefault)

  lazy val canadaRegexValidations: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(s"$keyword.${CountryCode.canada}", canadaRegexValidationsDefault)

  lazy val usaRegexValidations: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(s"$keyword.${CountryCode.usa}", usaRegexValidationsDefault)

  lazy val chileRegexValidations: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(s"$keyword.${CountryCode.chile}", chileRegexValidationsDefault)

  lazy val ecuadorRegexValidations: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(s"$keyword.${CountryCode.ecuador}", ecuadorRegexValidationsDefault)

  lazy val mexicoRegexValidations: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(s"$keyword.${CountryCode.mexico}", mexicoRegexValidationsDefault)

  def shuffleRegexValidations(
                               extraValidations: List[RegexValidation] = List.empty[RegexValidation]
                             ): List[RegexValidation] =
    Random.shuffle(
      extraValidations ++ chileRegexValidations ++ spainRegexValidations ++ franceRegexValidations ++
        ukRegexValidations ++ canadaRegexValidations ++ usaRegexValidations ++ ecuadorRegexValidations ++
        mexicoRegexValidations
    )

  // Overrides this but not really needed because we are overriding getRegexValidations
  override val defaultRegexValidations: List[RegexValidation] = List.empty

  override def getRegexValidations: List[RegexValidation] = shuffleRegexValidations()
}
