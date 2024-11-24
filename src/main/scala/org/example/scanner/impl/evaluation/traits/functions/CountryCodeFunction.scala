package org.example.scanner.impl.evaluation.traits.functions

import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.CountryCode
import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.evaluation.traits.provider.CountryCodeProvider
import scala.reflect.runtime.{currentMirror => cm}

private[impl] trait CountryCodeFunction extends RegexFunction {

  val countryCode: Option[CountryCodeType]

  private def getRegexValidationsFromProvider[CT <: CountryCodeProvider](clazz: Class[_]): List[RegexValidation] = {
    val companionModule = cm.classSymbol(clazz).companion.asModule
    val provider = cm.reflectModule(companionModule).instance.asInstanceOf[CT]
    countryCode match {
      case Some(CountryCode.spain)   => provider.spainRegexValidations
      case Some(CountryCode.uk)      => provider.ukRegexValidations
      case Some(CountryCode.france)  => provider.franceRegexValidations
      case Some(CountryCode.usa)     => provider.usaRegexValidations
      case Some(CountryCode.canada)  => provider.canadaRegexValidations
      case Some(CountryCode.chile)   => provider.chileRegexValidations
      case Some(CountryCode.ecuador) => provider.ecuadorRegexValidations
      case Some(CountryCode.mexico)  => provider.mexicoRegexValidations
      case _                         => provider.getRegexValidations
    }
  }

  override val regexValidations: List[RegexValidation] = getRegexValidationsFromProvider(this.getClass)

}