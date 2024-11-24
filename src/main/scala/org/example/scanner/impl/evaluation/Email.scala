package org.example.scanner.impl.evaluation

import org.example.scanner.impl.evaluation.Email.emailRegexStr
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.NoParamDSL
import org.example.scanner.sdk.types._

private[impl] case class Email() extends EvaluationFunction {

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)

  private val evaluator: RLike = RLike(emailRegexStr)

  override def evaluateDatum(
                              datum: Any
                            )(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val datumStr = datum.asInstanceOf[String]
    if (datumStr.contains('@')) evaluator.evaluateDatum(datumStr.toLowerCase) else NO_MATCH
  }

}

private[impl] object Email extends NoParamDSL[Email] {

  override val keyword: String = "email"

  private val emailRegexStr: String =
    "^\\s*(?=\\S)(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])\\s*$"

}
