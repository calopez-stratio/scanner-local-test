package org.example.scanner.impl.evaluation

import org.example.scanner.impl.model.{NLPModel, Prediction}
import org.example.scanner.impl.utils.StringUtils.StringImprovements
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, IGNORE_CONFIDENCE, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.NoParamDSL
import org.example.scanner.sdk.types._

private[impl] case class PersonName() extends EvaluationFunction {

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
  override val usesModel: Boolean = true

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val strDatum = datum.asInstanceOf[String]
    NLPModel.getPrediction(strDatum, System.identityHashCode(this)) match {
      case Some(predictions) if predictions.contains(Prediction.person) =>
        if (!strDatum.containsNumbersOrEspecialChars) HIGH_CONFIDENCE else NO_MATCH
      case None => IGNORE_CONFIDENCE
      case _    => NO_MATCH
    }
  }

}

private[impl] object PersonName extends NoParamDSL[PersonName] {

  override val keyword: String = "person_name"

}
