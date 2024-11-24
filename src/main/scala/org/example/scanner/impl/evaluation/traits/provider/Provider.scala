package org.example.scanner.impl.evaluation.traits.provider

import org.example.scanner.evaluation.RegexValidation

trait Provider {
  def getRegexValidations: List[RegexValidation]
}
