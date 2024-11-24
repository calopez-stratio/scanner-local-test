package org.example.scanner.impl.evaluation.traits.functions

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.evaluation.traits.provider.RegexProvider
import org.example.scanner.impl.utils.FunctionEvaluationUtils.evaluateFunctionConfidence
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.types.ScannerType

import scala.reflect.runtime.{currentMirror => cm}

private[impl] trait RegexFunction extends EvaluationFunction {

  private def getRegexValidationsFromProvider[CT <: RegexProvider](clazz: Class[_]): List[RegexValidation] = {
    val companionModule = cm.classSymbol(clazz).companion.asModule
    cm.reflectModule(companionModule).instance.asInstanceOf[CT].getRegexValidations
  }

  val regexValidations: List[RegexValidation] = getRegexValidationsFromProvider(this.getClass)

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence =
    datum match {
      case datum: String => evaluateFunctionConfidence(datum, regexValidations)
      case datum: Number => evaluateFunctionConfidence(datum.toString, regexValidations)
      case _             => NO_MATCH
    }

}
