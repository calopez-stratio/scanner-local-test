package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.traits.dsl.CountryCodeDSL
import org.example.scanner.impl.evaluation.traits.functions.CountryCodeFunction
import org.example.scanner.impl.evaluation.traits.provider.CountryCodeProvider
import org.example.scanner.impl.utils.Checksums
import org.example.scanner.sdk.ConfidenceEnum.{HIGH_CONFIDENCE, LOW_CONFIDENCE}
import org.example.scanner.sdk.traits.impl.FunctionDSL
import org.example.scanner.sdk.types.{ScannerType, ScannerTypes}

private[impl] case class AccountNumber(countryCode: Option[CountryCodeType]) extends CountryCodeFunction {
  override val supportedTypes: Seq[ScannerType] = ScannerTypes.INTEGER_STRING_TYPES
}

private[impl] object AccountNumber extends CountryCodeDSL[AccountNumber] with CountryCodeProvider {

  override val keyword: String = "account_number"

  override val enhancedColumnRegexDefault: FunctionDSL =
    "^(?!.*balance.*)(checking|savings|bank|debit|fund|holder|swift)?.*(account|acct|iban).*(number|no|num|#|information|details)?.*$"

  /**
   * ==Pattern==
   *   - Two - letter country code, including only the countries whose formatting is described in the IBAN Register IBAN
   *     https://www.iban.com/structure
   *   - Two check digits(followed by an optional space)
   *   - 1 to 7 groups of four letters or digits(can be separated by spaces)
   *   - 1 to 3 optional letters or digits
   *
   * ==Examples==
   * HR 17 2360 0001 1012 3456 555555555555555, BR15000000000, SV43ACAT00000000000000123123
   *
   * More info: https://www.morfoedro.it/doc.php?n=219&lang=en https://www.iban.com/structure
   */
  private val ibanRegexValidationDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex =
      "^(?>(ad|ae|al|at|az|ba|be|bi|br|bg|bh|by|ch|cr|cy|cz|de|dk|dj|do|ee|eg|es|fi|fo|fr|gb|ge|gt|gi|gl|gr|hr|hu|ie|il|is|it|iq|jo|kw|kz|lb|li|lc|lt|lu|ly|lv|mc|md|me|mk|mr|mt|mu|nl|no|pl|pk|ps|pt|qa|ro|rs|sa|sc|sd|se|si|st|sk|sm|sv|tn|tr|tl|ua|va|vg|xk|AD|AE|AL|AT|AZ|BA|BE|BI|BR|BG|BH|BY|CH|CR|CY|CZ|DE|DK|DJ|DO|EE|EG|ES|FI|FO|FR|GB|GE|GT|GI|GL|GR|HR|HU|IE|IL|IS|IT|IQ|JO|KW|KZ|LB|LI|LC|LT|LU|LY|LV|MC|MD|ME|MK|MR|MT|MU|NL|NO|PL|PK|PS|PT|QA|RO|RS|SA|SC|SD|SE|SI|ST|SK|SM|SV|TN|TR|TL|UA|VA|VG|XK))\\s?[0-9]{2}\\s?((?>[a-zA-Z0-9]{4}\\s?)){3,7}([a-zA-Z]{1,3}|[0-9]{1,3})?$"
        .r
  ))

  private lazy val ibanRegexValidation: List[RegexValidation] = ScannerConfig
    .getRegexValidationOrElse(s"$keyword.IBAN", ibanRegexValidationDefault)

  /**
   * ==Pattern==
   * An IBAN account starting with countrycode "ES" and length 24
   *
   * ==Examples==
   * ES7921000813610123456789
   */
  override val spainRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = "^(?>es|ES)\\s?[0-9]{2}\\s?((?>[a-zA-Z0-9]{4})\\s?){5}$".r)
  )

  /**
   * ==Pattern==
   * An IBAN account starting with countrycode "FR" and length 27
   *
   * ==Examples==
   * FR7630006000011234567890189
   */
  override val franceRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^(?>fr|FR)\\s?[0-9]{2}\\s?((?>[a-zA-Z0-9]{4})\\s?){5}([a-zA-Z]{3}|[0-9]{3})$".r
  ))

  /**
   * ==Pattern==
   * An IBAN account starting with countrycode "GB" and length 22
   *
   * ==Examples==
   * GB33BUKB20201555555555
   */
  override val ukRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^(?>gb|GB)\\s?[0-9]{2}\\s?((?>[a-zA-Z0-9]{4})\\s?){4}([0-9]{2})$".r
  ))

  /**
   * ==Pattern==
   * A Canada Bank Account Number is 7 or 12 digits A Canada bank account transit number is:
   *   - five digits
   *   - a hyphen
   *   - three digits or
   *   - a zero "0"
   *   - eight digits
   *
   * ==Examples==
   * 12345-123-12-12-12345
   *
   * More info:
   * https://www.cibc.com/en/personal-banking/ways-to-bank/how-to/transit-account-institution-number.html#:~:text=Ways%20to%20find%20your%20transit%20number%20and%20account%20number&text=Find%20the%20number%20associated%20with,is%20the%20bank%20account%20number.
   */
  override val canadaRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = "^[0-9]{5}-[0-9]{3}-[0-9]{2}-[0-9]{2}-?[0-9]{5}$".r),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^[0-9]{15}$".r)
  )

  /**
   * ==Pattern==
   * 6-17 consecutive digits
   *
   * ==Examples==
   * 123456, 88374634564566643
   */
  override val usaRegexValidationsDefault: List[RegexValidation] = List.empty

  /**
   * ==Pattern==
   * Several types of bank account formats depending on the bank:
   *   - Banco Estado: Uses RUT Account - Chilean RUT number without verifying digit
   *   - Chile, Bice, Corp, Scotiabank: 00-000-00000-00
   *   - Crédito: 00-00000-0
   *   - BBVA: 0000-0000000000
   *   - Estado: 00000000000
   *   - Santander: 000-00-00000-0
   *   - Banco del Desarrollo: 00000000000
   *   - Banco Security: 0-0000000-00
   *   - Banco Falabella: 0-000-000000-0
   *
   * ==Examples==
   * 12345678, 88374634564, 18-34445-6, 3992-2834570018
   */
  override val chileRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex =
      ("^([0-9]{2}-[0-9]{5}-[0-9]{1,2})|" + "([0-9]{4}-[0-9]{10})|" + "([0-9]{2}-[0-9]{3}-[0-9]{5}-[0-9]{2})|" +
        "([0-9]{3}-[0-9]{2}-[0-9]{5}-[0-9])|" + "([0-9]-[0-9]{7}-[0-9]{2})|" + "([0-9]-[0-9]{3}-[0-9]{6}-[0-9])$").r
  ))

  /**
   * ==Pattern==
   * 30 digits
   *
   * ==Examples==
   * 12345678901122334455667788990
   */

  override val ecuadorRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = LOW_CONFIDENCE, regex = "^[0-9]{30}$".r))

  /**
   * ==Pattern==
   * The CLABE (Clave Bancaria Estandarizada) is a standard for bank accounts in Mexico. CLABE format
   *
   * The 18 numeric digits of the CLABE:
   *   - 3 Digits: Bank Code
   *   - 3 Digits: Branch Office Code
   *   - 11 Digits: Account Number
   *   - 1 Digit : Control Digit
   *
   * ==Examples==
   * 032180000118359719
   */
  override val mexicoRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = "^[0-9]{18}$".r, checksumFunction = Checksums.clabeChecksum)
  )

  override lazy val getRegexValidations: List[RegexValidation] = shuffleRegexValidations(ibanRegexValidation)
}
