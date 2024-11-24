package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.traits.dsl.CountryCodeDSL
import org.example.scanner.impl.evaluation.traits.functions.CountryCodeFunction
import org.example.scanner.impl.evaluation.traits.provider.CountryCodeProvider
import org.example.scanner.impl.utils.{Checksums, RegexUtils}
import org.example.scanner.sdk.ConfidenceEnum.HIGH_CONFIDENCE
import org.example.scanner.sdk.traits.impl.FunctionDSL
import org.example.scanner.sdk.types._

private[impl] case class SocialSecurityNumber(countryCode: Option[CountryCodeType]) extends CountryCodeFunction {
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
}

private[impl] object SocialSecurityNumber extends CountryCodeDSL[SocialSecurityNumber] with CountryCodeProvider {

  override val keyword: String = "social_security_number"

  override val enhancedColumnRegexDefault: FunctionDSL =
    "^.*((social.*security|(social.*)?insurance).*(number|no|num|n|#|id|identification)?).*|.*(((numero|codigo|code)?.*seguridad|assurance|securit).*(social|soc).*)|.*(ssin|ssid|phin|ssns|sins|ssn|sin#).*$"

  /**
   * ==Pattern==
   * 11-12 numbers with optional slashes:
   *
   *   - two digits
   *   - a forward slash (optional)
   *   - seven to eight digitsS
   *   - two digits following controlKey checksum
   *
   * ==Examples==
   * 08/12345678, 281234567840
   *
   * More info: http://www.grupoalquerque.es/ferias/2012/archivos/digitos/codigo_seguridad_social.pdf
   */

  override val spainRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^(?>0[1-9]|[1-4][0-9]|5[0-3]|66)[-/]?[0-9]{7,8}(?>[0-8][0-9]|9[0-6])$".r,
    checksumFunction = Checksums.controlKeySpain,
    checksumCleanChars = List(' ', '-', '/')
  ))

  /**
   * ==Pattern==
   * 13 digit number + a control key: syymmlloookkk cc
   *
   *   - s: 1 for a male, 2 for a female
   *   - yy: last two digits of the year of birth
   *   - mm: month of birth, 20 if unknown
   *   - ll: number of the department of origin * Metropolitan France: 2 digits, or 1 digit and 1 letter * Overseas: 3
   *     digits
   *   - ooo: commune of origin (a department is composed of various communes) * Metropolitan France: 3 digits *
   *     Overseas: 2 digits
   *   - kkk: order number to distinguish people being born at the same place in the same year and month
   *   - cc: control key Checksum
   *
   * ==Examples==
   * 180126955222380, 283-209-921-625-930, 1801269552223 80
   *
   * More info:
   * https://www.familysearch.org/en/wiki/Insee_Number#:~:text=Each%20French%20person%20receives%20at,%2B%20a%20two%2Ddigit%20key
   */
  override val franceRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = RegexUtils.combineRegex(List(
      "([1-2][0-9]{2}-(?>0[1-9]|1[0-2]|20)[0-9]-[0-9abAB][0-9]{2}-[0-9]{3}-[0-9](?>[0-8][0-9]|9[0-7]))".r,
      "([1-2][0-9]{2}-(?>0[1-9]|1[0-2]|20)[0-9]-[0-9]{3}-[0-9]{3}-[0-9](?>[0-8][0-9]|9[0-7]))".r,
      "([1-2][0-9]{2}(?>0[1-9]|1[0-2]|20)[0-9][0-9abAB][0-9]{6} ?(?>[0-8][0-9]|9[0-7]))".r,
      "([1-2][0-9]{2}(?>0[1-9]|1[0-2]|20)[0-9]{8} ?(?>[0-8][0-9]|9[0-7]))".r
    )),
    checksumFunction = Checksums.controlKeyFrance,
    checksumCleanChars = List(' ', '-')
  ))

  /**
   * ==Pattern==
   * two possible patterns, unformatted and formatted:
   *
   *   - two letters (valid NINOs use only certain characters in this prefix, which this pattern validates; not
   *     case-sensitive)
   *   - six digits
   *   - either 'A', 'B', 'C', or 'D' (like the prefix, only certain characters are allowed in the suffix; not
   *     case-sensitive)
   *
   * OR
   *
   *   - two letters
   *   - a space or dash
   *   - two digits
   *   - a space or dash
   *   - two digits
   *   - a space or dash
   *   - two digits
   *   - a space or dash
   *   - either 'A', 'B', 'C', or 'D'
   *
   * ==Examples==
   * AX123456A, AX-12-34-56-A, AX 12 34 56 A
   *
   * More info: https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110
   */

  override val ukRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex =
      "^(?>((?!bg|gb|kn|nk|nt|tn|zz|BG|GB|KN|NK|NT|TN|ZZ|Bg|Gb|Kn|Nk|Nt|Tn|Zz|bG|gB|kN|nK|nT|tN|zZ)[abceghj-prstwxyzABCEGHJ-PRSTWXYZ][abceghj-nprstwxyzABCEGHJ-NPRSTWXYZ])-[0-9]{2}-[0-9]{2}-[0-9]{2}-[a-dA-D])|(((?!bg|gb|kn|nk|nt|tn|zz|BG|GB|KN|NK|NT|TN|ZZ|Bg|Gb|Kn|Nk|Nt|Tn|Zz|bG|gB|kN|nK|nT|tN|zZ)[abceghj-prstwxyzABCEGHJ-PRSTWXYZ][abceghj-nprstwxyzABCEGHJ-NPRSTWXYZ]) [0-9]{2} [0-9]{2} [0-9]{2} [a-dA-D])|((?>((?!bg|gb|kn|nk|nt|tn|zz|BG|GB|KN|NK|NT|TN|ZZ|Bg|Gb|Kn|Nk|Nt|Tn|Zz|bG|gB|kN|nK|nT|tN|zZ)[abceghj-prstwxyzABCEGHJ-PRSTWXYZ][abceghj-nprstwxyzABCEGHJ-NPRSTWXYZ]))[0-9]{6}[a-dA-D])$"
        .r
  ))

  /**
   * ==Pattern==
   * Nine digits and Luhn Checksum
   *
   * ==Examples==
   * 046454286, 511-700-957, 293443438
   *
   * More info: https://en.wikipedia.org/wiki/Social_insurance_number
   */
  override val canadaRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^([0-9]{3}[- ]?){2}[0-9]{3}$".r,
    checksumFunction = Checksums.luhnChecksum,
    checksumCleanChars = List('-', ' ')
  ))

  /**
   * ==Pattern==
   * Nine digits commonly written as three fields separated with hyphens AAA-GG-SSSS
   *
   * ==Examples==
   * 276-33-1924, 001-21-1199, 777810928
   *
   * More info:
   * https://www.usrecordsearch.com/ssn.htm#:~:text=A%20Social%20Security%20Number%20(SSN,called%20the%20%22serial%20number%22.
   */
  override val usaRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = HIGH_CONFIDENCE, regex = "^[0-9]{3}-[0-9]{2}-[0-9]{4}$".r))

  override val chileRegexValidationsDefault: List[RegexValidation] = List.empty

  override val ecuadorRegexValidationsDefault: List[RegexValidation] = List.empty

  /**
   * ==Pattern==
   * The Mexican SSN is made of 11 digits that have the following format:
   *   - Two digits: Represents the IMSS delegation in which the SSN was registered
   *   - Two digits: Year of the worker registration
   *   - Two digits: Year of birth of the worker
   *   - Four digits
   *   - Check digit: Check digit for the Luhn algorithm
   *
   * More info:
   * https://es.stackoverflow.com/questions/32023/c%C3%B3mo-validar-un-n%C3%BAmero-de-seguridad-social-nss-de-m%C3%A9xico
   * ==Examples==
   * 046454286, 511-700-957, 293443438
   */
  override val mexicoRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = "^[0-9]{11}$".r, checksumFunction = Checksums.luhnChecksum)
  )

}
