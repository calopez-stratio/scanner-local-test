package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.traits.dsl.CountryCodeDSL
import org.example.scanner.impl.evaluation.traits.functions.CountryCodeFunction
import org.example.scanner.impl.evaluation.traits.provider.CountryCodeProvider
import org.example.scanner.impl.utils.Checksums
import org.example.scanner.sdk.ConfidenceEnum.{HIGH_CONFIDENCE, LOW_CONFIDENCE}
import org.example.scanner.sdk.traits.impl.FunctionDSL
import org.example.scanner.sdk.types.{ScannerType, ScannerTypes}

private[impl] case class IdentificationNumber(countryCode: Option[CountryCodeType]) extends CountryCodeFunction {
  override val supportedTypes: Seq[ScannerType] = ScannerTypes.INTEGER_STRING_TYPES
}

private[impl] object IdentificationNumber extends CountryCodeDSL[IdentificationNumber] with CountryCodeProvider {

  override val keyword: String = "id_number"

  override val enhancedColumnRegexDefault: FunctionDSL =
    "^.*((national)?.*(identification|id|nir).*(number|no|num)).*|.*(numer|document|carne).*(nacional)?.*identidad.*|(dni|nino|nie|pesel)$"

  /**
   * ==Pattern==
   * one to two digits an optional period three digits an optional period three digits a dash one digit or letter (not
   * case-sensitive) which is a check digit
   *
   * ==Examples==
   * 05.576.281-7, 055762817, 8.959.296-k, 8959296k
   */
  override val chileRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^(?>[0-9]{1,2}[. ]?[0-9]{3}[. ]?[0-9]{3}[ -]?[0-9kK])$".r,
    checksumFunction = Checksums.modulo11,
    checksumCleanChars = List('.', '-')
  ))

  /**
   * ==Pattern==
   * Spanish DNI is formed by seven digits followed by one character:
   *   - eight digits
   *   - an optional space or hyphen
   *   - one check letter (not case-sensitive)
   * ==Examples==
   * 51503856W
   */
  override val spainRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^(?>[KLMXYZklmxyz0-9][0-9]{7}[ -]?[A-Za-z])$".r,
    checksumFunction = Checksums.dniChecksum,
    checksumCleanChars = List(' ', '-')
  ))

  /**
   * ==Pattern==
   * The id document number in United Kingdom is the National Insurance Number (NINO).
   * ==Examples==
   * QQ 12 34 56 A
   * ==Sources==
   * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110
   */
  override val ukRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex =
      "^(?>((?!bg|gb|kn|nk|nt|tn|zz|BG|GB|KN|NK|NT|TN|ZZ|Bg|Gb|Kn|Nk|Nt|Tn|Zz|bG|gB|kN|nK|nT|tN|zZ)[abceghj-prstwxyzABCEGHJ-PRSTWXYZ][abceghj-nprstwxyzABCEGHJ-NPRSTWXYZ])-[0-9]{2}-[0-9]{2}-[0-9]{2}-[a-dA-D])|(((?!bg|gb|kn|nk|nt|tn|zz|BG|GB|KN|NK|NT|TN|ZZ|Bg|Gb|Kn|Nk|Nt|Tn|Zz|bG|gB|kN|nK|nT|tN|zZ)[abceghj-prstwxyzABCEGHJ-PRSTWXYZ][abceghj-nprstwxyzABCEGHJ-NPRSTWXYZ]) [0-9]{2} [0-9]{2} [0-9]{2} [a-dA-D])|((?>((?!bg|gb|kn|nk|nt|tn|zz|BG|GB|KN|NK|NT|TN|ZZ|Bg|Gb|Kn|Nk|Nt|Tn|Zz|bG|gB|kN|nK|nT|tN|zZ)[abceghj-prstwxyzABCEGHJ-PRSTWXYZ][abceghj-nprstwxyzABCEGHJ-NPRSTWXYZ]))[0-9]{6}[a-dA-D])$"
        .r
  ))

  /**
   * ==Pattern==
   * 9 or 12 digits and uppercase letters
   *
   * ==Examples==
   * X4RTBPFW4, D2H6862M2
   *
   * ==Sources==
   * https://learn.microsoft.com/en-us/purview/sit-defn-france-national-id-card
   */
  override val franceRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(regex = "^([0-9A-Z]{9}|[0-9A-Z]{12})$".r, confidence = LOW_CONFIDENCE))

  /**
   * ==Pattern==
   * The id document number in Canada is the Social Security Number
   */
  override val canadaRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^([0-9]{3}[- ]?){2}[0-9]{3}$".r,
    checksumFunction = Checksums.luhnChecksum,
    checksumCleanChars = List('-', '/')
  ))

  /**
   * ==Pattern==
   * The id document number in USA is the Social Security Number
   */
  override val usaRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = HIGH_CONFIDENCE, regex = "^[0-9]{3}-[0-9]{2}-[0-9]{4}$".r))

  /**
   * ==Pattern==
   * The identification number in Ecuador is called Cédula de Identidad and it is composed of 10 digits as follows:
   *   - 2 digits: Corresponding to the province number between 1 and 24 or 30 for foreigners
   *   - 1 digit: Takes a value between 0 and 5
   *   - 6 digits: Its a sequential number padded with 0s
   *   - 1 digit: Check digit
   *
   * ==Examples==
   * 1220103616, 1739150652
   */
  override val ecuadorRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^0*([0-1]?[0-9]|2[0-4]|30)[0-5][0-9]{6}-?[0-9]$".r,
    checksumFunction = Checksums.modulo10
  ))

  /**
   * ==Pattern==
   * The CURP (Clave Única de Registro de Población) code is composed of 18 characters that are assigned as follows:
   *   - The first surname's initial and first inside vowel;
   *   - The second surname's initial (or the letter "X" if, like some foreign nationals, the person has no second
   *     surname)
   *   - The first given name's initial
   *   - Date of birth (2 digits for year, 2 digits for month, and 2 digits for day)
   *   - A one-letter gender indicator (H for male (hombre in Spanish), M for female (mujer in Spanish), or X for
   *     non-binary)
   *   - A two-letter code for the state where the person was born for persons born abroad, the code NE (nacido en el
   *     extranjero) is used
   *   - The first surname's first inside consonant
   *   - The second surname's first inside consonant
   *   - The first given name's first inside consonant and
   *   - One character ranging from 0-9 for people born before 2000 or from A-Z for people born since 2000 this is
   *     generated by the National Population Registry to prevent identical entries.
   *   - Control digit, which checks the validity of the previous 17 digits
   *   - For married women, only maiden names are used.
   *
   * Exceptions Several exceptions to the above rules exist, including:
   *   - Ñ: If any step in the above procedure of getting the consonants leads to the letter "Ñ" appearing anywhere in
   *     the CURP, the "Ñ" is replaced by an "X"
   *   - If there is no maternal or paternal surname, the letter X is used
   *
   * ==Examples==
   * MOCA990728MCMRVN09, coga670304masddd20, oeaf771012hmcrgr09, GOTA820521HVZMLP02, HEGG560427MVZRRL04
   */
  override val mexicoRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex =
      "^[a-z][aeiouAEIOU][a-zA-Z]{2}[0-9]{2}(0[1-9]|1[0-2])([0-2][1-9]|3[01])(h|m|x|H|M|X)(as|bc|bs|cc|cl|cm|cs|ch|df|dg|gt|gr|hg|jc|mc|mn|ms|nt|nl|oc|pl|qt|qr|sp|sl|sr|tc|ts|tl|vz|yn|zs|ne|AS|BC|BS|CC|CL|CM|CS|CH|DF|DG|GT|GR|HG|JC|MC|MN|MS|NT|NL|OC|PL|QT|QR|SP|SL|SR|TC|TS|TL|VZ|YN|ZS|NE)[bcdfghjklmnpqrstvwxyzBCDFGHJKLMNPQRSTVWXYZ]{3}[0-9a-zA-Z][0-9]$"
        .r,
    checksumFunction = Checksums.curpChecksum
  ))

}
