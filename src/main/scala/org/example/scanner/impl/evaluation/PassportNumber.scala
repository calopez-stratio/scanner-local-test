package org.example.scanner.impl.evaluation

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.traits.dsl.CountryCodeDSL
import org.example.scanner.impl.evaluation.traits.functions.CountryCodeFunction
import org.example.scanner.impl.evaluation.traits.provider.CountryCodeProvider
import org.example.scanner.impl.utils.Checksums
import org.example.scanner.sdk.ConfidenceEnum.{HIGH_CONFIDENCE, LOW_CONFIDENCE, MEDIUM_CONFIDENCE}
import org.example.scanner.sdk.traits.impl.FunctionDSL
import org.example.scanner.sdk.types.{ScannerType, ScannerTypes}

private[impl] case class PassportNumber(countryCode: Option[CountryCodeType]) extends CountryCodeFunction {
  override val supportedTypes: Seq[ScannerType] = ScannerTypes.INTEGER_STRING_TYPES
}

private[impl] object PassportNumber extends CountryCodeDSL[PassportNumber] with CountryCodeProvider {

  override val keyword = "passport_number"

  override val enhancedColumnRegexDefault: FunctionDSL = "^.*(passport|passeport).*(number|no|num|n)?.*$"

  /**
   * ==Pattern==
   * A nine-character combination of letters and numbers:
   *   - three letters
   *   - six digits
   *
   * ==Examples==
   * FGH123456, ABC123456
   *
   * ==Source==
   * https://www.bbva.es/finanzas-vistazo/ef/finanzas-personales/cual-es-el-numero-de-pasaporte-en-espanol-y-como-se-obtiene.html
   * http://www.migoia.com/migoia/util/NIF/NIF.pdf https://www.uma.es/media/tinyimages/file/Composicion__NIF_2014.pdf
   * http://www.juntadeandalucia.es/servicios/madeja/contenido/recurso/677
   */
  override val spainRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[a-zA-Z]{3}[0-9]{6})$".r))

  /**
   * ==Pattern==
   * Nine digits and letters:
   *   - two digits
   *   - two letters (not case-sensitive)
   *   - five digits
   *
   * ==Examples==
   * 12ab12345, 34AB67891
   *
   * ==Sources==
   * https://docs.trellix.com/es-ES/bundle/data-loss-prevention-11.10.x-classification-definitions-reference-guide/page/GUID-07CC7D5B-ABA6-44EE-8AE0-B92F30966476.html
   * https://learn.microsoft.com/es-es/purview/sit-defn-france-passport-number
   */
  override val franceRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[0-9]{2}[a-zA-Z]{2}[0-9]{5})$".r))

  /**
   * ==Pattern==
   *   - one letter or digit
   *   - eight digits
   *
   * ==Examples==
   * 123456789
   *
   * ==Sources==
   * https://learn.microsoft.com/es-es/purview/sit-defn-us-uk-passport-number
   */
  override val usaRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>[a-zA-Z0-9][0-9]{8})$".r))

  /**
   * ==Pattern==
   *   - one letter or digit
   *   - eight digits
   *
   * ==Examples==
   * 123456789
   *
   * ==Sources==
   * https://learn.microsoft.com/es-es/purview/sit-defn-us-uk-passport-number
   */
  override val ukRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = LOW_CONFIDENCE, regex = "^(?>[a-zA-Z0-9][0-9]{8})$".r))

  /**
   * ==Pattern==
   * Two uppercase letters followed by six digits
   *
   * ==Examples==
   * AB123456
   *
   * ==Sources==
   * https://learn.microsoft.com/es-es/purview/sit-defn-canada-passport-number
   */
  override val canadaRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[A-Z]{2}[0-9]{6})$".r))

  /**
   * ==Pattern==
   * One uppercase letter followed by six digits
   *
   * ==Examples==
   * A123456
   *
   * ==Sources==
   * https://www.workingholiday.cl/el-nuevo-pasaporte/
   */
  override val chileRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>[A-Z]{2}[0-9]{6})$".r))

  /**
   * ==Pattern==
   * Ecuatorian passport has changed since 2020, so there are currently two coexistent formats:
   *   - Passport before 2020: The same number as the Ecuatorian Cédula de Identidad. It is composed of 10 digits with a
   *     checksum
   *   - Passport after 2020: Letter A followed by 7 digits
   *
   * ==Examples==
   * 1220103616, A123456
   *
   * ==Sources==
   * https://www.eluniverso.com/noticias/ecuador/conozca-los-detalles-y-el-numero-que-identifica-al-pasaporte-ecuatoriano-nota/
   */
  override val ecuadorRegexValidationsDefault: List[RegexValidation] = List(
    // Ecuador ID number check
    RegexValidation(
      confidence = HIGH_CONFIDENCE,
      regex = "^0*([0-1]?[0-9]|2[0-4]|30)[0-5][0-9]{7}$".r,
      checksumFunction = Checksums.modulo10
    ),
    // Ecuador Passport number check
    RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>(A|a)[a-zA-Z0-9]{2}[0-9]{4})$".r)
  )

  /**
   * ==Pattern==
   * Mexican passport number has the following format:
   *   - Letter N
   *   - Eight digits
   *
   * ==Examples==
   * N12345678
   *
   * ==Sources==
   * https://dof.gob.mx/nota_detalle.php?codigo=5630645&fecha=23/09/2021#gsc.tab=0
   */
  override val mexicoRegexValidationsDefault: List[RegexValidation] =
    List(RegexValidation(confidence = MEDIUM_CONFIDENCE, regex = "^(?>N[0-9]{8})$".r))

}
