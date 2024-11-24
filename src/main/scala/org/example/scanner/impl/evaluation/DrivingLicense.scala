package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.traits.dsl.CountryCodeDSL
import org.example.scanner.impl.evaluation.traits.functions.CountryCodeFunction
import org.example.scanner.impl.evaluation.traits.provider.CountryCodeProvider
import org.example.scanner.impl.utils.Checksums
import org.example.scanner.sdk.ConfidenceEnum.{HIGH_CONFIDENCE, LOW_CONFIDENCE, MEDIUM_CONFIDENCE}
import org.example.scanner.sdk.types._

import scala.util.matching.Regex

private[impl] case class DrivingLicense(countryCode: Option[CountryCodeType]) extends CountryCodeFunction {
  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
}

private[impl] object DrivingLicense extends CountryCodeDSL[DrivingLicense] with CountryCodeProvider {

  override val keyword: String = "driving_license"

  override val enhancedColumnRegexDefault: String =
    "^.*driv.*(license|lic|card|permit).*|.*(licencia|license|carnet|permis).*(manejo|conduc|conduc|conduire).*$"

  /**
   * ==Pattern==
   * The US is formed by 50 States and the District of Columbia. The driving license varies across all of them:
   *
   *   - State of Montana driver's licenses are nine numbers. They also may be 13 digits in length:
   *     - First 2 digits = the month of birth
   *     - Next 3 digits = randomly assigned
   *     - Next 4 digits = the year of birth
   *     - Next 2 digits = “41”
   *     - Last 2 digits = day of birth
   *     - Format: #########, MM###YYYY41DD
   *     - Example: 626926253, 0636219994123
   *   - State of New Hampshire driver's license numbers are 10 numbers.
   *     - First 2 characters = the month of birth
   *     - Next 3 characters = the first and last letters of the last name and the first initial of the first name
   *     - Next 2 digits = the year of birth
   *     - Next 2 digits = the day of birth
   *     - Final digit = prevents code duplication.
   *     - Format: MMAAAYYDDF
   *     - Example: 06ASD9923
   *   - State of Washington driver's license numbers use the format: three letters ** two letters, three numbers, one
   *     letter, and one number.
   *     - Format: AAA**AA###A#
   *     - Example: DOE**MJ501P1
   *   - States of Alabama, Alaska, Arkansas, Connecticut, Delaware, Georgia, Louisiana, Maine, Nevada, New Mexico,
   *     Oregon, South Carolina, South Dakota, Tennessee, Texas, Utah, Washington, D.C. and Rhode Island driving
   *     licenses are numeric from 7 to 10 length numbers or length 12 in the case of North Carolina
   *     - Format: ########
   *     - Example: 12345678
   *   - States of Arizona, Hawaii, Massachusetts, Missouri, Nebraska, Oklahoma, West Virginia driving licenses are 7 to
   *     10 length string, or 13 in the case of Georgia, Florida, Michigan, Minnesota and Maryland starting by a letter
   *     followed by numbers
   *     - Format: A#########
   *     - Example: W123456789
   *   - States of Idaho, Iowa and North Dakota are 9 length alphanumeric with the format AA######A, ###AA#### or
   *     AAA######
   *   - States of Ohio and Vermont are 8 length alphanumeric with the format AA######, or #######A Example: TL545796,
   *     8205059A
   *   - States of Florida, Maryland and Michigan driver licenses can also be length 16 or 17 formatted as five groups
   *     separated by dash or spaces with one group of one character at the beginning or at the end
   *     - Format: A##-###-##-###-# or A-###-###-###-###
   *     - Example: A55-444-33-222-1, A-444-333-222-111
   *   - State of Wisconsin is length 17 formed by one letter and 13 numbers formatted, separated by dashes.
   *     - Format: L###-####-####-##
   *     - Example: J525-4209-0465-05.
   *   - State of New Jersey driver's license numbers are length 17 and consist of one letter and 14 numbers separated
   *     in three groups by dashes where The first character is the first initial of the last name Next 9 digits are
   *     assigned Last 5 digits = the month and year of birth and a code for eye color (50 is added to the birth month
   *     for females)
   *     - Format: L####-#####-#####
   *     - Example: C3828-19953-54895
   *   - States of Kansas, Kentucky, Virginia and Illinois driving licenses are three groups of formatted numbers with
   *     length of 11 or 14 separated by dashes or spaces
   *     - Format: L##-##-####, L##-###-###, L##-##-####, L###-####-####
   *     - Example: H23-33-3453. L45-567-986. K44-56-6778
   *   - States of Colorado, Indiana, Mississippi, New York, Pennsylvania driving licenses are three groups of formatted
   *     numbers with lengths from 10 to 12 separated by dashes or spaces.
   *     - Format: ##-###-####, ####-##-####, ###-##-####, ###-###-###, ### ### ###, ## ### ###
   *     - Example: 94-33-0101, 0299-11-6078, 125-01-2050, 712-345-678, 123 456 789, 99 900 104
   *   - State of Wyoming driving licenses are two groups of numbers separated by dash
   *     - Format: ######-##
   *     - Example: 050070-003
   */
  private val ArizonaStateDrivingLicenseRegex: Regex = "^(?>([a-zA-Z][0-9]{8})|([0-9]{9}))$".r
  private val ArkansasStateDrivingLicenseRegex: Regex = "^(?>9[0-9]{8})$".r
  private val CaliforniaStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z][0-9]{7})$".r
  private val ColoradoStateDrivingLicenseRegex: Regex = "^(?>[0-9]{2}-[0-9]{3}-[0-9]{4})$".r

  private val FloridaStateDrivingLicenseFormattedRegex: Regex =
    "^(?>([a-zA-Z][0-9]{3}-[0-9]{3}-[0-9]{2}-[0-9]{3}-[0-9])|([a-zA-Z]-[0-9]{3}-[0-9]{3}-[0-9]{3}-[0-9]{3}))$".r

  private val FloridaStateDrivingLicenseUnformattedRegex: Regex = "^(?>[a-zA-Z][0-9]{12})$".r
  private val HawaiiStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z][0-9]{8})$".r
  private val IdahoStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z]{2}[0-9]{6}[a-zA-Z])$".r
  private val IllinoisStateDrivingLicenseFormattedRegex: Regex = "^(?>[a-zA-Z][0-9]{3}-[0-9]{4}-[0-9]{4})$".r
  private val IndianaStateDrivingLicenseRegex: Regex = "^(?>[0-9]{4}-[0-9]{2}-[0-9]{4})$".r
  private val IowaStateDrivingLicenseRegex: Regex = "^(?>([0-9]{3}[a-zA-Z]{2}[0-9]{4})|([0-9]{9}))$".r
  private val KansasStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z][0-9]{2}-[0-9]{2}-[0-9]{4})$".r
  private val KentuckyStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z][0-9]{2}-[0-9]{3}-[0-9]{3})$".r
  private val MarylandStateDrivingLicenseFormattedRegex: Regex = "^(?>[a-zA-Z]-[0-9]{3}-[0-9]{3}-[0-9]{3}-[0-9]{3})$".r
  private val MarylandStateDrivingLicenseUnformattedRegex: Regex = "^(?>[a-zA-Z][0-9]{12})$".r
  private val MichiganStateDrivingLicenseFormattedRegex: Regex = "^(?>[a-zA-Z]-[0-9]{3}-[0-9]{3}-[0-9]{3}-[0-9]{3})$".r
  private val MichiganStateDrivingLicenseUnformattedRegex: Regex = "^(?>[a-zA-Z][0-9]{12})$".r
  private val MinnesotaStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z][0-9]{12})$".r
  private val MississippiStateDrivingLicenseRegex: Regex = "^(?>[0-9]{3}-[0-9]{2}-[0-9]{4})$".r
  private val MissouriStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z][0-9]{9})$".r
  private val MontanaStateDrivingLicenseRegex: Regex = "^(?>(0[1-9]|1[0-2])[0-9]{7}41(0[1-9]|[1-2][0-9]|3[0-1]))$".r
  private val NebraskaStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z][0-9]{8})$".r

  private val NewHampshireStateDrivingLicenseRegex: Regex =
    "^(?>(0[1-9]|1[0-2])[a-zA-Z]{3}[0-9]{2}(0[1-9]|[1-2][0-9]|3[0-1])[0-9])$".r

  private val NewJerseyStateDrivingLicenseRegex: Regex =
    "^(?>[a-zA-Z][0-9]{4}-[0-9]{5}-((0[1-9]|1[0-2])|(5[1-9]|6[0-2]))[0-9]{2}[1-6])$".r

  private val NewYorkStateDrivingLicenseRegex: Regex = "^(?>[0-9]{3} [0-9]{3} [0-9]{3})$".r
  private val NorthDakotaStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z]{3}[0-9]{2}[0-9]{4})$".r
  private val OhioStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z]{2}[0-9]{6})$".r
  private val OklahomaStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z][0-9]{9})$".r
  private val PennsylvaniaStateDrivingLicenseRegex: Regex = "^(?>[0-9]{2} [0-9]{3} [0-9]{3})$".r
  private val RhodeIslandStateDrivingLicenseRegex: Regex = "^(?>[1-9]{2}[0-9]{5})$".r
  private val VermontStateDrivingLicenseRegex: Regex = "^(?>([0-9]{7}[a-zA-Z])|([0-9]{8}))$".r
  private val VirginiaStateDrivingLicenseFormattedRegex: Regex = "^(?>[a-zA-Z][0-9]{2}-[0-9]{2}-[0-9]{4})$".r
  private val VirginiaStateDrivingLicenseUnformattedRegex: Regex = "^(?>[a-zA-Z][0-9]{8})$".r
  private val WashingtonStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z]{3}\\*\\*[a-zA-Z]{2}[0-9]{3}[a-zA-Z][0-9])$".r
  private val WestVirginiaStateDrivingLicenseRegex: Regex = "^(?>([a-zA-Z][0-9]{6})|([0-9]{7}))$".r
  private val WisconsinStateDrivingLicenseRegex: Regex = "^(?>[a-zA-Z][0-9]{3}-[0-9]{4}-[0-9]{4}-[0-9]{2})$".r
  private val WyomingStateDrivingLicenseRegex: Regex = "^(?>[0-9]{6}-[0-9]{3})$".r

  override val usaRegexValidationsDefault: List[RegexValidation] = List(
    // High confidence Regexes
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = MontanaStateDrivingLicenseRegex),
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = NewJerseyStateDrivingLicenseRegex),
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = WashingtonStateDrivingLicenseRegex),
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = NewHampshireStateDrivingLicenseRegex),
    // Medium confidence Regexes
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = WisconsinStateDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = FloridaStateDrivingLicenseFormattedRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = MarylandStateDrivingLicenseFormattedRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = MichiganStateDrivingLicenseFormattedRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = KansasStateDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = KentuckyStateDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = VirginiaStateDrivingLicenseFormattedRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = IllinoisStateDrivingLicenseFormattedRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = ColoradoStateDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = IndianaStateDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = MississippiStateDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = NewYorkStateDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = PennsylvaniaStateDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = WyomingStateDrivingLicenseRegex),
    // Low confidence Regexes
    RegexValidation(confidence = LOW_CONFIDENCE, regex = ArizonaStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = ArkansasStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = CaliforniaStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = FloridaStateDrivingLicenseUnformattedRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = HawaiiStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = IdahoStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = IowaStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = MarylandStateDrivingLicenseUnformattedRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = MichiganStateDrivingLicenseUnformattedRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = MinnesotaStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = MissouriStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = NebraskaStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = NorthDakotaStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = OhioStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = OklahomaStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = RhodeIslandStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = VermontStateDrivingLicenseRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = VirginiaStateDrivingLicenseUnformattedRegex),
    RegexValidation(confidence = LOW_CONFIDENCE, regex = WestVirginiaStateDrivingLicenseRegex)
  )

  /**
   * ==Pattern==
   * Canada is formed by ten provinces and three territories. Each one has a different driving license format. The
   * shortest format is 6 digits long and the longest format is 17 digits long:
   *
   *   - British Columbia, New Brunswick, Northwest Territories, Nunavut, Yukon, Saskatchewan driving licenses are 6-8
   *     numbers.
   *     - Format: ########, ######
   *     - Example: 12345678, 993826
   *   - Alberta driving license is a length 6 digit and first 1-2 o digits can be letters.
   *     - Format: AA####, A#####
   *     - Example: FJ3718, O90826
   *   - State of Newfoundland & Labrador driving license can be 2 letters and 8 numbers or a letter and the date of
   *     birth in the format YYMMDD followed by three numbers.
   *     - Format: AYYMMDD###, AA########
   *     - Example: A011225111, UE88827362
   *   - State of Prince Edward Island driving license is four numbers followed by the date of birth in the format
   *     DDMMYY and ending with two numbers
   *     - Format: ####DDMMYY##
   *     - Example: 777701058709
   *   - State of Prince Edward Island driving license is four numbers followed by the date of birth in the format
   *     DDMMYY and ending with two numbers
   *     - Format: ####DDMMYY##
   *     - Example: 7777010587
   *   - State of Quebec is similar to Prince Edward Island driving license, adding the first letter of the last name at
   *     the beginning
   *     - Format: A####DDMMYY##
   *     - Example: R7777010587
   *   - State of Nova Scotia driving license is formed by the first 5 letters of your the name, the date of birth in
   *     the format DDMMYY and three numbers
   *     - Format: AAAAADDMMYY###
   *     - Example: JOHNS230495789
   *   - State of Ontario driving license is formed by the first letter of the last name, eight numbers and the birth
   *     date of the person in the format YYMMDD. It can also be formatted, separated with dashes in groups of 5 digits
   *     - Format: A####-####Y-YMMDD, A########YYMMDD
   *     - Example: K1234-56788-91108, K12345678891108
   */
  private val AlbertaProvinceDrivingLicenseRegex: Regex = "^(?>([a-zA-Z]{2}[0-9]{4})|([a-zA-Z][0-9]{5}))$".r

  private val NewflandLabradorProvinceDrivingLicenseRegex: Regex =
    "^(?>([a-zA-Z]{2}[0-9]{8})|([a-zA-Z][0-9]{2}(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])[0-9]{3}))$".r

  private val QuebecProvinceDrivingLicenseRegex: Regex =
    "^(?>[a-zA-Z][0-9]{4}(0[1-9]|[1-2][0-9]|3[0-1])(0[1-9]|1[0-2])[0-9]{4})$".r

  private val NovaScotiaProvinceDrivingLicenseRegex: Regex =
    "^(?>[a-zA-Z]{5}(0[1-9]|[1-2][0-9]|3[0-1])(0[1-9]|1[0-2])[0-9]{5})$".r

  private val OntarioProvinceDrivingLicenseUnformatRegex: Regex =
    "^(?>[a-zA-Z][0-9]{10}(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1]))$".r

  private val OntarioProvinceDrivingLicenseFormattedRegex: Regex =
    "^(?>[a-zA-Z][0-9]{4}-[0-9]{5}-[0-9](0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1]))$".r

  override val canadaRegexValidationsDefault: List[RegexValidation] = List(
    // High confidence Regex
    RegexValidation(confidence = HIGH_CONFIDENCE, regex = OntarioProvinceDrivingLicenseFormattedRegex),
    // Medium confidence Regexes
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = NewflandLabradorProvinceDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = NovaScotiaProvinceDrivingLicenseRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = OntarioProvinceDrivingLicenseUnformatRegex),
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = QuebecProvinceDrivingLicenseRegex),
    // Low confidence Regexes
    RegexValidation(confidence = LOW_CONFIDENCE, regex = AlbertaProvinceDrivingLicenseRegex)
  )

  /**
   * ==Pattern==
   * 18 letters and digits:
   *
   *   - 1–5: The first five characters of the surname (padded with 9s if fewer than 5 characters)
   *   - 6: The decade digit from the year of birth (e.g. for 1987 it would be 8)
   *   - 7–8: The month of birth in two digit format (7th character is incremented by 5 if the driver is female i.e.
   *     51–62 instead of 01–12)
   *   - 9–10: The date within the day of birth in two digit format (i.e. 01–31)
   *   - 11: The year digit from the year of birth (e.g. for 1987 it would be 7)
   *   - 12–13: The first initial of the first and middle name, padded with a 9 if no middle name (e.g. for John Doe
   *     Smith JD, for Jane Smith J9)
   *   - 14: Arbitrary digit – usually 9, but decremented to differentiate drivers with the first 13 characters in
   *     common
   *   - 15–16: Two computer check digits.
   *   - 17–18: Two digits representing the licence issue, which increases by 1 for each licence issued
   *
   * Each Northern Ireland license holder has a unique driver number which is 8 characters long. The characters are not
   * constructed in any particular pattern.
   *
   * https://en.wikipedia.org/wiki/Driving_licence_in_the_United_Kingdom#:~:text=Driver%20numbers,-Each%20licence%20holder&text=1%E2%80%935%3A%20The%20first%20five,as%20%22MC%22%20for%20all.&text=15%E2%80%9316%3A%20Two%20computer%20check,1%20for%20each%20licence%20issued.
   *
   * ==Examples==
   * MORGA753116SM9IJ 35, MORGA753116SM9IJ35, 88822266
   */
  private val UkDrivingLicenseRegex: Regex =
    "^(?>[a-zA-Z9]{5}[0-9]((0[1-9]|1[0-2])|(5[1-9]|6[0-2]))(0[1-9]|[1-2][0-9]|3[0-1])[0-9][a-zA-Z9]{2}[0-9][a-zA-Z]{2}[0-9]{2})$"
      .r

  override val ukRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = HIGH_CONFIDENCE, regex = UkDrivingLicenseRegex))

  /**
   * ==Pattern==
   * Spanish Driving License is the same as the National Identification Number (DNI)
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
   * France driving licenses can be:
   *   - If obtained before 1st June 1975, 15/13-length alphanumeric string ending with 4 numbers
   *   - If obtained after 1st June 1975, a sequence of 12 numbers
   * ==Examples==
   * 274659201117, 377738501846, 2777192749J7436
   */
  override val franceRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[a-zA-Z0-9]{9,11}[0-9]{4})$".r))

  /**
   * ==Pattern==
   * Chilean driving licenses are the national identification number called Rol Único Nacional (RUN)
   * ==Examples==
   * 05.576.281-7
   */
  override val chileRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex = "^(?>[0-9]{1,2}[. ]?[0-9]{3}[. ]?[0-9]{3}[ -]?[0-9kK])$".r,
    checksumFunction = Checksums.modulo11,
    checksumCleanChars = List('.', '-')
  ))

  /**
   * ==Pattern==
   * Ecuatorian driving license is formed by 10 numbers. It can be formatted: 9 numbers a dash and 1 number.
   *
   * ==Examples==
   * 170646608-2, 170456789-8, 1802832608, 0301854295, 1207240738
   */
  override val ecuadorRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[0-9]{9}-?[0-9])$".r))

  /**
   * ==Pattern==
   * There is a different driving license for each one of the States of Mexico. Driving licenses also contain the CURP.
   * However, due to the lack of existent information of Mexican driving licenses, it has been decided to use CURP as a
   * valid identification.
   */
  override val mexicoRegexValidationsDefault: List[RegexValidation] = List(RegexValidation(
    confidence = HIGH_CONFIDENCE,
    regex =
      "^[a-z][aeiou][a-z]{2}[0-9]{2}(0[1-9]|1[0-2])([0-2][1-9]|3[01])[hmx](as|bc|bs|cc|cl|cm|cs|ch|df|dg|gt|gr|hg|jc|mc|mn|ms|nt|nl|oc|pl|qt|qr|sp|sl|sr|tc|ts|tl|vz|yn|zs|ne)[bcdfghjklmnpqrstvwxyz]{3}[0-9a-z][0-9]$"
        .r,
    checksumFunction = Checksums.curpChecksum,
    checksumFailConfidence = MEDIUM_CONFIDENCE
  ))

}
