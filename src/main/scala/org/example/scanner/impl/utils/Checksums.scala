package org.example.scanner.impl.utils

import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.{AccountNumber, CountryCode, IdentificationNumber, SocialSecurityNumber, TaxIdentifier}
import org.example.scanner.impl.utils.StringUtils.StringImprovements

import scala.collection.immutable.HashMap

object Checksums {

  type ChecksumFunction = String => Boolean

  private val digitalRoot: Seq[Int] = Seq(0, 0, 1, 2, 2, 4, 3, 6, 4, 8, 5, 1, 6, 3, 7, 5, 8, 7, 9, 9)

  val checksumsMap: Map[String, ChecksumFunction] = Map(
    s"${SocialSecurityNumber.keyword}_${CountryCode.spain}" -> controlKeySpain,
    s"${SocialSecurityNumber.keyword}_${CountryCode.france}" -> controlKeyFrance,
    s"${IdentificationNumber.keyword}_${CountryCode.spain}" -> dniChecksum,
    s"${IdentificationNumber.keyword}_${CountryCode.mexico}" -> curpChecksum,
    s"${TaxIdentifier.keyword}_${CountryCode.spain}" -> nifChecksum,
    s"${TaxIdentifier.keyword}_${CountryCode.france}" -> controlKeyFranceTax,
    s"${TaxIdentifier.keyword}_${CountryCode.ecuador}_RUC" -> modulo10Ecuador,
    s"${TaxIdentifier.keyword}_${CountryCode.ecuador}" -> modulo11Ecuador,
    s"${AccountNumber.keyword}_${CountryCode.mexico}" -> clabeChecksum,
    "luhn_checksum" -> luhnChecksum,
    "modulo_10" -> modulo10,
    "modulo_11" -> modulo11
  )

  def luhnChecksum: ChecksumFunction =
    _.map(_.asDigit).reverse.zipWithIndex.map { case (digit, index) => digitalRoot(digit * 2 + index % 2) }.sum % 10 ==
      0

  /*
   "control key" in social security number is a 00 to 96 number
   equal to 97-(the rest of the number modulo 97) or to 97 if the number is a multiple of 97
   */
  def controlKeyFrance: ChecksumFunction =
    (number: String) => {
      val control = number.substring(number.length - 2).toInt
      val checkNum = BigInt(
        number.substring(0, number.length - 2).replace('a', '0').replace('b', '0').replace('A', '0').replace('B', '0')
      )
      control >= 1 && control <= 97 && control == (97 - (checkNum % 97))
    }

  /*
   The French key is calculated as follow :
   Key = [ 12 + 3 * ( SIREN (last 9 digits) modulo 97 ) ] modulo 97

   For example  : Key = [ 12 + 3 * ( 404,833,048 modulo 97 ) ] modulo 97 = [12 + 3*56] modulo 97 = 180 modulo 97 = 83 so the tax number for 404,833,048 is FR 83,404,833,048
   */
  def controlKeyFranceTax: ChecksumFunction =
    (number: String) => {
      val control = number.substring(2, 4).toInt
      val checkNum = BigInt(number.substring(4, number.length))
      control == (12 + 3 * (checkNum % 97)) % 97
    }

  /*
   "control key" in social security number is a 00 to 96 number
   equal to 97-(the rest of the number modulo 97) or to 97 if the number is a multiple of 97
   */
  def controlKeySpain: ChecksumFunction =
    (number: String) => {
      val control = number.substring(number.length - 2).toInt
      val checkNum = BigInt(number.substring(0, number.length - 2))
      control >= 0 && control <= 96 && control == checkNum % 97
    }

  def controlKeyGenerator(number: String, countryCode: CountryCodeType): String = {
    val checkNum = BigInt(
      number.substring(0, number.length - 2).toLowerCase.replace('a', '0').replace('b', '0').removeChar(' ', '-', '/')
    )
    countryCode match {
      case CountryCode.france => "%02d".format(97 - (checkNum % 97))
      case CountryCode.spain  => "%02d".format(checkNum % 97)
    }
  }

  def dniChecksum: ChecksumFunction =
    (number: String) => {
      val checkDigit = number(number.length - 1).toLower
      dniChecksumGenerator(number) == checkDigit
    }

  def dniChecksumGenerator(number: String): Char = {
    val numericPart = number.substring(0, number.length - 1).toLowerCase()
    val first = numericPart.head match {
      case 'x' => "0"
      case 'y' => "1"
      case 'z' => "2"
      case 'k' => ""
      case 'l' => ""
      case 'm' => ""
      case s   => s
    }
    val checkNum = (first + numericPart.substring(1, 8)).toInt

    val letterMatch = HashMap(
      0 -> 't',
      1 -> 'r',
      2 -> 'w',
      3 -> 'a',
      4 -> 'g',
      5 -> 'm',
      6 -> 'y',
      7 -> 'f',
      8 -> 'p',
      9 -> 'd',
      10 -> 'x',
      11 -> 'b',
      12 -> 'n',
      13 -> 'j',
      14 -> 'z',
      15 -> 's',
      16 -> 'q',
      17 -> 'v',
      18 -> 'h',
      19 -> 'l',
      20 -> 'c',
      21 -> 'k',
      22 -> 'e'
    ): HashMap[Int, Char]

    val res = checkNum % 23

    letterMatch(res)
  }

  /*
   Checks if the number is a valid NIE or Spanish tax identifier number
   More information: http://www.migoia.com/migoia/util/NIF/NIF.pdf
   */

  def nifChecksum: ChecksumFunction =
    (number: String) => {
      val control = number(8).toLower
      nifChecksumGenerator(number) contains control

    }

  def nifChecksumGenerator(number: String): Seq[Char] = {
    val entityType = number(1).toLower
    val checkNum = number.substring(1, 8)

    val sum = checkNum
      .zipWithIndex
      .map {
        case (digit, idx) =>
          val d = digit.toString.toInt
          if ((idx + 1) % 2 != 0) d * 2 else d
      }
      .flatMap(d => d.toString.map(_.asDigit))
      .sum

    val res = (10 - (sum % 10)) % 10

    // Letter
    if (entityType == 'p' || entityType == 'q' || entityType == 'r' || entityType == 's' || entityType == 'w') {
      Seq((97 + (res - 1 + 10) % 10).toChar)
    } // Digit
    else if (entityType == 'a' || entityType == 'b' || entityType == 'e' || entityType == 'h') {
      Seq(res.toString()(0))
    } else { Seq(res.toString()(0), (97 + (res - 1 + 10) % 10).toChar) }

  }

  /*
   Used for Chile RUN
   More information: https://es.wikipedia.org/wiki/C%C3%B3digo_de_control
   */

  def modulo11: ChecksumFunction =
    (number: String) => {
      val checkDigit = number(number.length - 1).toLower
      modulo11Generator(number) == checkDigit
    }

  def modulo11Generator(number: String): Char = {
    val numericPart = number.substring(0, number.length - 1)

    val weights = Stream.continually(2 to 7).flatten
    val sum = numericPart.reverse.zip(weights).map { case (digit, weight) => digit.asDigit * weight }.sum
    val modulo = sum % 11
    val res = 11 - modulo

    res match {
      case r if r < 10 => (r + 48).toChar
      case 10          => 'k'
      case 11          => '0'
    }
  }

  /*
   Used for Ecuador RUC
   More information: https://www.jybaro.com/blog/cedula-de-identidad-ecuatoriana/#Verificacion_de_RUC
   */

  def modulo11Ecuador: ChecksumFunction =
    (number: String) => {
      // Pad with 0s until the string is of length 13
      val paddedNumber = number.adjustLength(13, '0')
      val thirdDigit = paddedNumber(2)
      val numericPartEnd = thirdDigit match {
        case '6' => 5
        case '9' => 4
        case _   => 0
      }
      val checkDigit = paddedNumber(paddedNumber.length - numericPartEnd)

      modulo11EcuadorGenerator(paddedNumber) == checkDigit
    }

  def modulo11EcuadorGenerator(number: String): Char = {
    val thirdDigit = number(2)
    val numericPartEnd = thirdDigit match {
      case '6' => 5
      case '9' => 4
      case _   => 0
    }

    val numericPart = number.substring(0, number.length - numericPartEnd)

    val publicInstitutionWeights = Stream(3, 2, 7, 6, 5, 4, 3, 2)
    val legalPersonWeights = Stream(4, 3, 2, 7, 6, 5, 4, 3, 2)

    val sum = numericPart
      .zip(if (thirdDigit.equals('6')) publicInstitutionWeights else legalPersonWeights)
      .map { case (digit, weight) => digit.asDigit * weight }
      .sum
    val modulo = sum % 11
    val res = 11 - modulo

    res match {
      case r if r < 10 => (r + 48).toChar
      case 10          => 'X'
      case 11          => '0'
    }
  }

  /*
   Used for Ecuador Cédula de Identidad
   More information: https://www.jybaro.com/blog/cedula-de-identidad-ecuatoriana/#Algoritmo_de_Verificacion
   */

  def modulo10: ChecksumFunction =
    (number: String) => {
      val checkDigit = number(number.length - 1)
      // Pad with 0s until the string is of length 10
      val paddedNumber = number.adjustLength(10, '0')
      modulo10Generator(paddedNumber) == checkDigit
    }

  def modulo10Ecuador: ChecksumFunction =
    (number: String) => {
      val checkDigit = number(number.length - 4)
      // Pad with 0s until the string is of length 13
      val paddedNumber = number.adjustLength(13, '0')
      modulo10Generator(paddedNumber, 4) == checkDigit
    }

  def modulo10Generator(number: String, positionCheckDigit: Int = 1): Char = {
    val numericPart = number.substring(0, number.length - positionCheckDigit)
    val odd = numericPart.zipWithIndex.filter(_._2 % 2 != 0).map(_._1.asDigit)
    val even = numericPart
      .zipWithIndex
      .filter(_._2 % 2 == 0)
      .map {
        e =>
          val res = e._1.asDigit * 2
          if (res > 9) res - 9 else res
      }
    val sumaTotal = even.sum + odd.sum
    val chr = ((10 - (sumaTotal % 10)) % 10 + 48).toChar
    chr
  }

  def curpChecksum: ChecksumFunction =
    (number: String) => {
      val checkDigit = number(number.length - 1)
      curpChecksumGenerator(number) == checkDigit
    }

  def curpChecksumGenerator(number: String): Char = {
    val numericPart = number.substring(0, number.length - 1)
    val table = (('0' to '9') ++ "abcdefghijklmnñopqrstuvwxyz".toCharArray).zipWithIndex.toMap
    val res = numericPart
      .reverse
      .zip(Stream.from(2))
      .map {
        case (char, position) =>
          val tableValue = table.getOrElse(char, 0)
          tableValue * position
      }
      .sum % 10

    ((10 - res) % 10) + 48 toChar
  }

  /*
    Used for Mexico bank account CLABE verification.
    More information: https://bank.codes/mexico-clabe-checker/
   */
  def clabeChecksum: ChecksumFunction =
    (number: String) => {
      val checkDigit = number(number.length - 1)
      clabeChecksumGenerator(number) == checkDigit
    }

  def clabeChecksumGenerator(number: String): Char = {
    val numericPart = number.substring(0, number.length - 1)
    val res = Seq(3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7)
      .zip(numericPart)
      .map { case (multiplier, number) => (multiplier * number.asDigit) % 10 }
      .sum % 10

    ((10 - res) % 10) + 48 toChar
  }

}
