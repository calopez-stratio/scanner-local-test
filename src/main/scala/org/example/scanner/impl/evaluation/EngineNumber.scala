package org.example.scanner.impl.evaluation

import org.example.scanner.impl.model.{NLPModel, Prediction}
import org.example.scanner.sdk.ConfidenceEnum._
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.NoParamDSL
import org.example.scanner.sdk.types._

private[impl] case class EngineNumber() extends EvaluationFunction {

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
  override val usesModel: Boolean = true

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val strDatum = datum.asInstanceOf[String]
    NLPModel.getPrediction(strDatum, System.identityHashCode(this)) match {
      case Some(predictions) if predictions.contains(Prediction.engineNumber) && strDatum.length < 8 => LOW_CONFIDENCE
      case Some(predictions) if predictions.contains(Prediction.engineNumber) && strDatum.length < 12 =>
        MEDIUM_CONFIDENCE
      case Some(predictions) if predictions.contains(Prediction.engineNumber) => HIGH_CONFIDENCE
      case Some(predictions) if predictions.isEmpty                           => NO_MATCH
      case None                                                               => IGNORE_CONFIDENCE
      case _                                                                  => NO_MATCH
    }
  }

}

private[impl] object EngineNumber extends NoParamDSL[EngineNumber] {

  override val keyword: String = "engine_number_nlp"

}
