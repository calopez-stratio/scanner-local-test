package org.example.scanner.impl.utils

import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.utils.StringUtils.StringImprovements
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, NO_MATCH, maxConfidence}

import scala.util.Try

object FunctionEvaluationUtils {

  /**
   * Evaluates the confidence of a given datum using a list of confidence checks.
   *
   * @param datum
   *   The datum to evaluate.
   * @param regexValidations
   *   The list of confidence checks to apply.
   * @return
   *   The highest confidence level achieved from the confidence checks.
   */
  def evaluateFunctionConfidence(datum: String, regexValidations: List[RegexValidation]): Confidence =
    regexValidations.foldLeft(NO_MATCH) {
      (highestConfidence: Confidence, regexValidation: RegexValidation) =>
        val expectedConfidence = regexValidation.confidence
        // If the highest confidence found is higher than the expected confidence then we have finished
        if (highestConfidence >= expectedConfidence) { highestConfidence }
        // Otherwise, evaluate the current RegexValidation
        else {
          val currentConfidence = evaluateRegexValidation(datum, regexValidation)
          // Update the highest confidence
          maxConfidence(currentConfidence, highestConfidence)
        }
    }

  /**
   * Evaluates the confidence of a given datum against a confidence check.
   *
   * @param datum
   *   The datum to be evaluated.
   * @param regexValidation
   *   The confidence check to be applied.
   * @return
   *   The confidence level of the evaluation.
   */
  private def evaluateRegexValidation(datum: String, regexValidation: RegexValidation): Confidence =
    // Datum matches the regex
    regexValidation
      .regex
      .findFirstIn(datum)
      .map(_ => evaluateChecksumFunction(datum, regexValidation))
      .getOrElse(NO_MATCH)

  /**
   * Evaluate a possible checksum against a datum.
   *
   * @param datum
   *   The datum to be evaluated.
   * @param regexValidation
   *   The confidence check to be applied.
   * @return
   *   The confidence level of the evaluation.
   */
  private def evaluateChecksumFunction(datum: String, regexValidation: RegexValidation): Confidence =
    if (regexValidation.checksumEnable) {
      // Clean the datum for the checksum
      val datumClean = datum.removeChar(regexValidation.checksumCleanChars: _*)
      // If checksum passes return the expected confidence
      if (Try(regexValidation.checksumFunction(datumClean)).toOption.contains(true)) regexValidation.confidence
      // If checksum does not match return fail confidence
      else regexValidation.checksumFailConfidence
    }
    // If there is no checksum return the expected confidence
    else regexValidation.confidence

}
