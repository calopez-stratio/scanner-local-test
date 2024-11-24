package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.traits.dsl.CountryCodeDSL
import org.example.scanner.impl.evaluation.traits.functions.CountryCodeFunction
import org.example.scanner.impl.evaluation.traits.provider.CountryCodeProvider
import org.example.scanner.impl.utils.Checksums
import org.example.scanner.sdk.ConfidenceEnum.{HIGH_CONFIDENCE, LOW_CONFIDENCE, MEDIUM_CONFIDENCE}
import org.example.scanner.sdk.types.{ScannerType, ScannerTypes}

private[impl] case class TaxIdentifier(countryCode: Option[CountryCodeType]) extends CountryCodeFunction {
  override val supportedTypes: Seq[ScannerType] = ScannerTypes.INTEGER_STRING_TYPES
}

private[impl] object TaxIdentifier extends CountryCodeDSL[TaxIdentifier] with CountryCodeProvider {

  override val keyword: String = "tax_identifier"

  override val enhancedColumnRegexDefault: String =
    "^.*(tax.*(number|no|#|num|id|payer|identification).*)|(numero.*(identifica(cion|ção|tion))?.*(fisca|contribuyente).*)|(tin|itin|cif|vat|nif).*$"

  /*
  Additional info for EU countries VAT number: https://en.wikipedia.org/wiki/VAT_identification_number#:~:text=11%20digit%20number%20formed%20from,modulus%2089%20check%20digit%20calculation.
   */

  /**
   * ==Pattern==
   * The TIN (Número de Identificación Fiscal) for individuals is their spanish DNI. For entities NIF or CIF (before
   * 2008) consists of 9 characters:
   *   - 1 letter A, B, C, D, E, F, G, H, J, N, P, Q, R, S, U, V or W for Spanish entities, N for foreign entities and W
   *     for permanent establishments of a non-resident in Spain)
   *   - 7 digits
   *   - 1 alphanumeric character matching the checksum
   *
   * Optional ES at the beginning for the VAT number
   *
   * ==Examples==
   * A08001851, L8372630, B29805314, X0000000T, 51503856W, A79075438, W0041692E
   *
   * More info: http://www.migoia.com/migoia/util/NIF/NIF.pdf
   * https://www.uma.es/media/tinyimages/file/Composicion__NIF_2014.pdf
   * http://www.juntadeandalucia.es/servicios/madeja/contenido/recurso/677
   */

  override val spainRegexValidationsDefault: List[RegexValidation] = List(
    // Spain tax ID checks
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^(?>(es|ES)?[abcdefghjnpqrsuvwABCDEFGHJNPQRSUVW][0-9]{7}[0-9a-zA-Z])$".r,
      checksumFunction = Checksums.nifChecksum
    ),
    // Spain ID number checks
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^(?>(es|ES)?[KLMXYZklmxyz0-9][0-9]{7}[ -]?[A-Za-z])$".r,
      checksumFunction = Checksums.dniChecksum,
      checksumCleanChars = List(' ', '-')
    )
  )

  /**
   * ==Pattern==
   * US valid ITINs are a nine-digit number in the same format as the SSN (9XX-8X-XXXX), begins with a “9” and the 4th
   * and 5th digits range from 50 to 65, 70 to 88, 90 to 92, and 94 to 99. The format is the following:
   *
   *   - The digit "9"
   *   - Two digits
   *   - A space, dash or nothing
   *   - A "7" or "8"
   *   - A digit
   *   - A space, dash or nothing
   *   - Four digits
   *
   * ==Regex==
   * {{{^9[0-9]{2}[ -]?[78][0-9][ -]?[0-9]{4}$}}}
   *
   * ==Examples==
   * 912811234, 912-71-1234, , 912 71 1234
   *
   * More info: https://www.irs.gov/pub/irs-pdf/p4757.pdf
   */
  override val usaRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = MEDIUM_CONFIDENCE,
      regex = "^9[0-9]{2}(?>(-[78][0-9]-[0-9]{4})|( [78][0-9] [0-9]{4}))$".r
    ),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>9[0-9]{2}[78][0-9]{5})$".r)
  )

  /**
   * ==Pattern==
   * ===Individuals===
   * Canadian Social Insurance Number (SIN)
   * ===Corporations===
   * Nine-digit Business Number (BN)
   * ===Trusts===
   * TXXXXXXXX where X is any number between 0 and 9
   *
   * ==Examples==
   * 123456789, T12345678
   *
   * More
   * info:https://www2.gov.bc.ca/gov/content/employment-business/business/managing-a-business/permits-licences/businesses-incorporated-companies/business-number#:~:text=The%20Canada%20Revenue%20Agency%20(CRA,it's%20doing%20business%20or%20operating.
   */
  override val canadaRegexValidationsDefault: List[RegexValidation] = List(
    // Canada SSNumber checks
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^([0-9]{3}[- ]?){2}[0-9]{3}$".r,
      checksumFunction = Checksums.luhnChecksum,
      checksumCleanChars = List('-', ' '),
      checksumFailConfidence = LOW_CONFIDENCE
    ),
    // Canada Tax ID checks
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^[0-9Tt][0-9]{8}$".r)
  )

  /**
   * ==Pattern==
   * Unique Taxpayer Reference (UTR) in UK has 10 numerals. Also Social Insurance number is valid
   *
   * However, also VAT numbers can be ised for tax identification purposes, depending on the company.
   *
   * Country code GB followed by either:
   *   - standard: 9 digits (block of 3, block of 4, block of 2 – e.g. GB999 9999 73)
   *   - branch traders: 12 digits (as for 9 digits, followed by a block of 3 digits)
   *   - government departments: the letters GD then 3 digits from 000 to 499 (e.g. GBGD001)
   *   - health authorities: the letters HA then 3 digits from 500 to 999 (e.g. GBHA599)
   *
   * If the company is located in Northern Ireland the prefix “XI” instead of GB will be used. For example, XI123456789.
   *
   * ==Examples==
   * 1234567890, XI123456789, GB999 9999 73
   */

  override val ukRegexValidationsDefault: List[RegexValidation] = List(
    // UK SS number checks
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^(?>((?!bg|gb|kn|nk|nt|tn|zz|BG|GB|KN|NK|NT|TN|ZZ|Bg|Gb|Kn|Nk|Nt|Tn|Zz|bG|gB|kN|nK|nT|tN|zZ)[abceghj-prstwxyzABCEGHJ-PRSTWXYZ][abceghj-nprstwxyzABCEGHJ-NPRSTWXYZ])-[0-9]{2}-[0-9]{2}-[0-9]{2}-[a-dA-D])|(((?!bg|gb|kn|nk|nt|tn|zz|BG|GB|KN|NK|NT|TN|ZZ|Bg|Gb|Kn|Nk|Nt|Tn|Zz|bG|gB|kN|nK|nT|tN|zZ)[abceghj-prstwxyzABCEGHJ-PRSTWXYZ][abceghj-nprstwxyzABCEGHJ-NPRSTWXYZ]) [0-9]{2} [0-9]{2} [0-9]{2} [a-dA-D])|((?>((?!bg|gb|kn|nk|nt|tn|zz|BG|GB|KN|NK|NT|TN|ZZ|Bg|Gb|Kn|Nk|Nt|Tn|Zz|bG|gB|kN|nK|nT|tN|zZ)[abceghj-prstwxyzABCEGHJ-PRSTWXYZ][abceghj-nprstwxyzABCEGHJ-NPRSTWXYZ]))[0-9]{6}[a-dA-D])$"
          .r
    ),
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^(GB|gb|XI|xi)(([0-9]{3} ?[0-9]{4} ?(([0-8][0-9])|(9[0-6]))( ?[0-9]{3})?)|((GD|gd)[0-4][0-9]{2})|((HA|ha)[5-9][0-9]{2}))$"
          .r
    ),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^[0-9]{10}$".r)
  )

  /**
   * ==Pattern==
   * 13 digits
   *
   *   - One digit that must be 0, 1, 2, or 3
   *   - One digit
   *   - A space (optional)
   *   - Two digits
   *   - A space (optional)
   *   - Three digits
   *   - A space (optional)
   *   - Three digits
   *   - A space (optional)
   *   - Three check digits //Impossible to find the checksum
   *
   * Also VAT number is valid. Its format is as follows: 'FR' + 2 digits (as validation key ) + 9 digits (as SIREN), the
   * first and/or the second value can also be a character – e.g. FRXX999999999
   *
   * The French key is calculated as follow :
   *
   * Key = [ 12 + 3 * ( SIREN modulo 97 ) ] modulo 97
   *
   * ==Examples==
   * 1234567890444, 1234 567 890 444, 12 34 567 890 444, FRXX999999999
   */

  override val franceRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^(FR|fr)[0-9]{11}$".r,
      checksumFunction = Checksums.controlKeyFranceTax
    ),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[0123][0-9] [0-9]{2} [0-9]{3} [0-9]{3} [0-9]{3})$".r),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>[0123][0-9]{12})$".r)
  )

  /**
   * ==Pattern==
   * Chilean tax identifiers are called Rol Único Tributario (RUT) and follow the same pattern as the national
   * identification number called Rol Único Nacional (RUN)
   *
   * ==Examples==
   * 05.576.281-7
   */

  // Chile ID number default checks
  override val chileRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^(?>[0-9]{1,2}\\.[0-9]{3}\\.[0-9]{3}-[0-9kK])$".r,
      checksumFunction = Checksums.modulo11,
      checksumCleanChars = List('.', '-')
    ),
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^(?>[0-9]{7,8}-?[0-9kK])$".r,
      checksumFunction = Checksums.modulo11,
      checksumCleanChars = List('-')
    )
  )

  /**
   * ==Pattern==
   * The tax identification number in Ecuador is known as the Registro Único de Contribuyentes (RUC). There are three
   * types of RUC according to the type of taxpayer:
   *   - '''Natural person''': it is the same identity card number of the person, plus the branch number (which is
   *     usually 001). Since it does not change to the identity card number, the third digit of this type of RUC will
   *     always be between zero (0) and five (5). Likewise, the verifying digit is calculated with module 10.
   *   - '''Legal person and foreigners without an identity card''': The structure of the first 10 digits is similar to
   *     that of the identity card, except for the third digit, which will always have a fixed value of nine (9). The
   *     last three digits are the establishment number (the first establishment is 001). Unlike the identity card, the
   *     tenth verification digit is calculated with modulo 11.
   *   - '''Public Institution''': The first two digits are the province code, as in the identity card. The third digit
   *     will always have the fixed value of six (6). The fourth to eighth digit is a sequence. The verifying digit is
   *     calculated with modulo 11 and is in the ninth position. The establishment number is the last four digits (the
   *     first establishment is 0001).
   *
   * ==Examples==
   *   - Natural person: 1713175071001
   *   - RUC of legal person: 1090072923001
   *   - Public institution: 1768028470001
   */

  override val ecuadorRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^0*([0-1]?[0-9]|2[0-4]|30)[0-5][0-9]{7}001$".r,
      checksumFunction = Checksums.modulo10Ecuador
    ),
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^0*([0-1]?[0-9]|2[0-4]|30)9[0-9]{7}001$".r,
      checksumFunction = Checksums.modulo11Ecuador
    ),
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^0*([0-1]?[0-9]|2[0-4]|30)6[0-9]{6}0001$".r,
      checksumFunction = Checksums.modulo11Ecuador
    )
  )

  /**
   * ==Pattern==
   * The TIN is called Clave en el Registro Federal de Contribuyentes - RFC and is issued by the Mexican Tax
   * Administration Service (SAT).
   *   - For individuals: It consists of 13 characters:
   *     - 2 characters: First letter + first vowel of the first surname
   *     - 1 character: First letter of the second surname
   *     - 1 character: First letter of the name
   *     - The next 6 digits are the date of birth (yymmdd)
   *     - The last 3 are the check digits (issued by SAT)
   *
   *   - For entities: It consists of 12 characters
   *     - The first 3 letters belong to the name of the company
   *     - The next 6 digits are the date of incorporation (yymmdd)
   *     - The last 3 are the check digits (issued by SAT)
   *
   * ==Examples==
   *   - RFC of a Natural person: XEXT990101NI4
   *   - RFC of an entity: EXT990101NI4
   */

  override val mexicoRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^[a-zA-Z]{3,4}[0-9]{2}(?>0[1-9]|1[0-2])(?>[0-2][1-9]|3[01])[a-zA-Z0-9]{3}$".r
  ))

}
