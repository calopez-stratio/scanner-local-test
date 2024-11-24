package org.example.scanner.evaluation

import org.example.scanner.impl.utils.Checksums.ChecksumFunction
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, NO_MATCH}

import scala.util.matching.Regex
case class RegexValidation(
                            confidence: Confidence,
                            regex: Regex,
                            checksumEnable: Boolean = true,
                            checksumFunction: ChecksumFunction = null,
                            checksumCleanChars: List[Char] = List.empty,
                            checksumFailConfidence: Confidence = NO_MATCH
                          ) {

  override def equals(obj: Any): Boolean =
    obj match {
      case that: RegexValidation => this.confidence == that.confidence &&
        this.regex.pattern.pattern() == that.regex.pattern.pattern() && this.checksumEnable == that.checksumEnable &&
        this.checksumFunction == that.checksumFunction && this.checksumCleanChars == that.checksumCleanChars &&
        this.checksumFailConfidence == that.checksumFailConfidence
      case _ => false
    }

}

object RegexValidation {

  def apply(confidence: Confidence, regex: Regex): RegexValidation =
    RegexValidation(confidence, regex, checksumEnable = false)

}
