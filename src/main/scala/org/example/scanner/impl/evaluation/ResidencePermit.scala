package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.traits.dsl.CountryCodeDSL
import org.example.scanner.impl.evaluation.traits.functions.CountryCodeFunction
import org.example.scanner.impl.evaluation.traits.provider.CountryCodeProvider
import org.example.scanner.sdk.ConfidenceEnum.{HIGH_CONFIDENCE, LOW_CONFIDENCE, MEDIUM_CONFIDENCE}
import org.example.scanner.sdk.traits.impl.FunctionDSL
import org.example.scanner.sdk.types._

private[impl] case class ResidencePermit(countryCode: Option[CountryCodeType]) extends CountryCodeFunction {
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
}

private[impl] object ResidencePermit extends CountryCodeDSL[ResidencePermit] with CountryCodeProvider {

  override val keyword: String = "residence_permit"

  override val enhancedColumnRegexDefault: FunctionDSL =
    "^.*residence.*(number|no|num|n|id|permit|card).*|.*(permis|titre|carte|numero).*(residenc|sejour|etranger|extranjero).*|(nie|brp|cds)$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

  /**
   * ==Pattern==
   * A letter C followed by eight digits
   *
   * ==Examples==
   * C02900966
   *
   * More info: https://taxdown.es/blog/numero-soporte-nie-obtener/
   */

  override val spainRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[cC][0-9]{8})$".r))

  /**
   * ==Pattern==
   * The Residence Permit in UK or Biometric residence permits (BRPs) format is:
   *   - Two letters
   *   - Either a ‘X’ or a digit
   *   - Six digits
   *
   * ==Examples==
   * ZW9005196, ABX005196
   *
   * More info:
   * https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/533854/BRP_OA_information_leaflet_-_July_2016.pdf
   */
  override val ukRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[a-zA-Z]{2}[xX0-9][0-9]{6})$".r))

  override val franceRegexValidationsDefault: List[RegexValidation] = List.empty

  /**
   * ==Pattern==
   * Thirteen digits with optional spaces
   *
   *   - three letters matching one of the codes below
   *
   *   - CSC – California Service Center
   *   - EAC – Eastern Adjudication Center (now known as Vermont Service Center)
   *   - IOE – ELIS (efile)
   *   - LIN – Lincoln Service Center (now known as Nebraska Service Center)
   *   - MSC – Missouri Service Center (now known as National Benefits Center)
   *   - NBC – National Benefits Center
   *   - NSC – Nebraska Service Center
   *   - SRC – Southern Regional Center (now known as Texas Service Center)
   *   - TSC – Texas Service Center
   *   - VSC – Vermont Service Center
   *   - WAC – Western Adjudication Center (now known as California Service Center)
   *   - YSC – Potomac Service Center
   *
   *   - ten numbers
   *
   * ==Examples==
   * CSC 17 012 34567, NSC2000182500
   *
   * More info: https://www.immigrationdirect.com/immigration-articles/permanent-resident-card-number/
   */

  override val usaRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^(?>(CSC|EAC|IOE|LIN|MSC|NBC|NSC|SRC|TSC|VSC|WAC|YSC) ?[0-9]{2} ?[0-9]{3} ?[0-9]{5})$".r
  ))

  /**
   * ==Pattern==
   * The Permanent Resident Card number can contain
   *   - two letters
   *   - ten numbers or
   *   - two letters
   *   - seven numbers
   *
   * ==Examples==
   * RA0302123456, PA0123456
   *
   * More info: https://services3.cic.gc.ca/ecas/redir.do?redir=id_num
   */
  override val canadaRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[a-zA-Z]{2}([0-9]{10}|[0-9]{7}))$".r))

  override val chileRegexValidationsDefault: List[RegexValidation] = List.empty

  override val ecuadorRegexValidationsDefault: List[RegexValidation] = List.empty

  /**
   * ==Pattern==
   * The Mexican Residence Permit Card number or Forma Migratoria Multiple (FMM) has the following format:
   *   - one letters
   *   - eight numbers
   *
   * ==Examples==
   * C12345678
   */
  override val mexicoRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>[a-zA-Z][0-9]{8})$".r))

}
