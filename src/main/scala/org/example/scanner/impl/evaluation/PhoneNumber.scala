package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.traits.dsl.CountryCodeDSL
import org.example.scanner.impl.evaluation.traits.functions.CountryCodeFunction
import org.example.scanner.impl.evaluation.traits.provider.CountryCodeProvider
import org.example.scanner.sdk.ConfidenceEnum.{HIGH_CONFIDENCE, LOW_CONFIDENCE, MEDIUM_CONFIDENCE}
import org.example.scanner.sdk.types._

private[impl] case class PhoneNumber(countryCode: Option[CountryCodeType]) extends CountryCodeFunction {
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
}

private[impl] object PhoneNumber extends CountryCodeDSL[PhoneNumber] with CountryCodeProvider {

  override val keyword: String = "phone_number"

  override val enhancedColumnRegexDefault: String =
    "^.*(phone|mobile|cell.*phone|call|contact|telf).*(number|no|num|#|n)?.*|(no|numero|num|#|n)?.*(telefono|telephone|telf|movil|mobile|celular|portable).*$"

  /**
   * ==Pattern==
   * This pattern is the same for USA and Canada phone numbers
   *   - 10 digit number, for e.g., +1 nxx-nxx-xxxx
   *   - Optional country code: +1
   *   - n can be any digit betweenKeyword 2-9
   *   - x can be any digit betweenKeyword 0-9
   *   - Optional parentheses around the area code: first nxx number
   *   - Optional space, / or - separator
   *   - Optional 4 digit extension
   * ==Examples==
   *   - Longest possible number: +1 (212) 212 1234 1234 -> lengthKeyword: 22
   *   - Shortest possible number: 2122121234 -> lengthKeyword: 10
   *   - +1 256-635-4563
   * ==Regex==
   * {{{^\+1[ -]?\(?[2-9][0-9]{2}\)?[ -/]?[2-9][0-9]{2}[ -/]?[0-9]{4}[ -/]?[0-9]{0,4}$}}}
   */

  override val usaRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = MEDIUM_CONFIDENCE,
      regex = "^(?>\\+1[ -]?\\(?[2-9][0-9]{2}\\)?[ \\-/]?[2-9][0-9]{2}[ \\-/]?[0-9]{4}[ \\-/]?[0-9]{0,4})$".r
    ),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>[2-9][0-9]{2}[2-9][0-9]{6,10})$".r)
  )

  /**
   * ==Pattern==
   * Canada phone numbers have the same format as USA phone numbers
   * ==Regex==
   * {{{^\+1?[ -]?\(?[2-9][0-9]{2}\)?[ -/]?[2-9][0-9]{2}[ -/]?[0-9]{4}[ -/]?[0-9]{0,4}$}}}
   */

  override val canadaRegexValidationsDefault: List[RegexValidation] = usaRegexValidationsDefault

  /**
   * ==Pattern==
   * Depends on the lengthKeyword of the area code:
   * ===3 Digit Area Code===
   *   - 0yy xxxx xxxx, (0yy) xxxx xxxx, +44 yy xxxx xxxx
   * ====Examples====
   *   - 021 2345 6789
   *   - (021) 2345 6789
   *   - +44 21 2345 6789
   *
   * ===4 Digit Area Code===
   *   - 0yyy xxx xxxx, (0yyy) xxx xxxx, +44 yyy xxx xxxx
   * ====Examples====
   *   - 0800 123 4567
   *   - (0800) 123 4567
   *   - +44 800 123 4567
   *
   * ===5 Digit Area Code===
   *   - 0yyyy xxx xxx, (0yyyy) xxx xxx, +44 yyyy xxxxxx
   * ====Examples====
   *   - 01234 123 456
   *   - (01234) 123 456
   *   - +44 1234 123 456
   *
   * Optional extension number with 3 or 4 digits after #
   *
   * ==Logic==
   *   - Longest possible number: +44 1234 123 456 #1234-> lengthKeyword: 22
   *   - Shortest possible number: 02112345678 -> lengthKeyword: 11
   *
   * ==Regex==
   * {{{^((?>(\+44 ?[0-9]{4}|\(0[0-9]{4}\)|0[0-9]{4}) ?[0-9]{3} ?[0-9]{3})|(?>(\+44 ?[0-9]{3}|\(?0[0-9]{3}\)?) ?[0-9]{3} ?[0-9]{4})|(?>(\+44 ?[0-9]{2}|\(?0[0-9]{2}\)?) ?[0-9]{4} ?[0-9]{4})|(\+44 ?[0-9]{4} ?[0-9]{4,5}))( #([0-9]{4}|[0-9]{3}))?$}}}
   *
   * ==Sources==
   * https://en.wikipedia.org/wiki/Telephone_numbers_in_the_United_Kingdom
   */

  override val ukRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^((?>(\\+44 ?[0-9]{4}|\\(0[0-9]{4}\\)) ?[0-9]{2,3} ?[0-9]{3})|(?>(\\+44 ?[0-9]{3}|\\(0[0-9]{3}\\)) ?[0-9]{3} ?[0-9]{4})|(?>(\\+44 ?[0-9]{2}|\\(0[0-9]{2}\\)) ?[0-9]{4} ?[0-9]{4})|(\\(0[0-9]{3} [0-9]{2}\\) ?[0-9]{4})|(\\+44 ?[0-9]{4} ?[0-9]{4,5}))( #([0-9]{4}|[0-9]{3}))?$"
          .r
    ),
    RegexValidation(
      confidence = MEDIUM_CONFIDENCE,
      regex =
        "^(?>(0[0-9]{4} [0-9]{4,6})|(0[0-9]{4} [0-9]{3} [0-9]{3})|(0[0-9]{3} [0-9]{3} [0-9]{3,4})|(0[0-9]{2} [0-9]{4} [0-9]{4}))$"
          .r
    ),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>0[0-9]{10})$".r)
  )

  /**
   * ==Pattern==
   * Mobile phone numbers begin with 6 or 7, followed by 8 digits (6xx xxx xxx or 7yx xxx xxx) For landline numbers, the
   * trunk prefix '9' is used followed by the province number 9xx xxx xxx Both (mobile and landline numbers) can be
   * preceded with the country prefix +34 or (34)
   *
   * ==Logic==
   *   - Longest possible number: (34) 693 73 34 30 or (34) 918 20 32 99-> lengthKeyword: 17
   *   - Shortest possible number: 693733430 -> lengthKeyword: 9
   *
   * ==Examples==
   * +34605647445, (34)913733430, 736772739, (+34)670392748
   */

  override val spainRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^(?>(\\(\\+34\\)|\\+34) ?[6-9][0-9]{2}[ -]?[0-9]{3}[ -]?[0-9]{3})$".r
    ),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>(34)?[6-9][0-9]{8})$".r)
  )

  /**
   * ==Pattern==
   * 10 digits
   *   - the first digit is a zero
   *   - the second digit expresses the type of number being used :
   *     - 1 is Paris
   *     - 2 to 4 are landlines (divided in 4 geographical zones)
   *     - 6 and 7 are mobile numbers
   *     - 8 are "special numbers" (free or premium-rate number)
   *     - Since 2006, 9 is used by web providers to offer landlines.
   *   - the rest depends on companies or numbers
   *
   * International prefix is +33
   *
   * ==Logic==
   *   - Longest possible number: (+33) 06 23 12 45 54 or (+33) 09 18 20 32 99-> lengthKeyword: 20
   *   - Shortest possible number: 0623124554 -> lengthKeyword: 10
   *
   * ==Examples==
   * +330556474459, (+33) 06 23 12 45 54, 0373677273, 05 56 10 20 30
   */

  override val franceRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^(?>(\\(\\+33\\)|\\+33)0?[0-9] ?[0-9]{2} ?[0-9]{2} ?[0-9]{2} ?[0-9]{2})$".r
    ),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>(0033|33)?0?[0-9]{9})$".r)
  )

  /**
   * ==Pattern==
   * Chilean phone numbers have the following format: +56 X XXXX XXXX Prefix could be written as 0056
   *
   * ==Logic==
   *   - Longest possible number: 0056 2 3298 8654 -> lengthKeyword: 17
   *   - Shortest possible number: +56232988654 -> lengthKeyword: 12
   *
   * ==Examples==
   * +56920203123
   */
  override val chileRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = "^(?>\\+56 ?[0-9] ?[0-9]{4} ?[0-9]{4})$".r),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>(0056|56)?[0-9]{9})$".r)
  )

  /**
   * ==Pattern==
   * Ecuatorian phone numbers have the following format: +593 X XXX XXXX. Prefix could be written as 0593 or 00593, or
   * can be written without prefix. It can be formatted with spaces or dashes.
   *
   * If the prefix is not specified:
   *   - Local calls: 7 digits of the subscriber xxx-xxxx
   *   - National long-distance calls: first a zero followed by the landline area code (0a) xxx-xxxx
   *
   * ==Logic==
   *   - Prefix: 593
   *   - Landline area code: 1 digit in the range 2 to 7
   *   - Landline number: 7 digits
   *
   * ==Examples==
   * +593 4 123 4567, +593-4-211-1111, 00593-4-211-1111, (02) 211-1111
   */

  override val ecuadorRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex =
        "^(>?(\\+593-?[0-9][ -]?[0-9]{3}[ -]?[0-9]{4})|(\\([0-9]{2}\\)[ -]?[0-9]{3}[ -]?[0-9]{4})|([0-9]{3}-[0-9]{4}))$"
          .r
    ),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(593)? ?[0-9]{2} ?[0-9]{6,7}$".r)
  )

  /**
   * ==Pattern==
   * Mexican phone numbers have the following format: +52 XX XXXX XXXX. Prefix could be written as 00 52 or 0052, or can
   * be written without prefix.
   *
   * ==Examples==
   * +52 55 1234 5678, 00 52 55 1234 5678, 0052 55 1234 5678
   */

  override val mexicoRegexValidationsDefault: List[RegexValidation] = List(
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = "^(?>\\+52 ?[0-9]{2} ?[0-9]{4} ?[0-9]{4})$".r),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>(0052)?[0-9]{10})$".r)
  )

}
