package org.example.scanner.impl.evaluation

import org.example.scanner.impl.model.{NLPModel, Prediction}
import org.example.scanner.sdk.ConfidenceEnum._
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.NoParamDSL
import org.example.scanner.sdk.types._

import scala.util.matching.Regex

private[impl] case class Username() extends EvaluationFunction {

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
  override val usesModel: Boolean = true

  private lazy val emailRegexStr: Regex =
    "(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])"
      .r

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val strDatum = datum.asInstanceOf[String]
    NLPModel.getPrediction(strDatum, System.identityHashCode(this)) match {
      case Some(predictions)
        if predictions.contains(Prediction.username) && emailRegexStr.findFirstIn(strDatum).isEmpty => HIGH_CONFIDENCE
      case Some(predictions) if predictions.isEmpty => NO_MATCH
      case None                                     => IGNORE_CONFIDENCE
      case _                                        => NO_MATCH
    }
  }

}

private[impl] object Username extends NoParamDSL[Username] {

  override val keyword: String = "username"

}
