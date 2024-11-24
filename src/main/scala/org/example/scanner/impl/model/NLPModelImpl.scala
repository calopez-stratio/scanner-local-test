package org.example.scanner.impl.model

import org.example.scanner.impl.model.Prediction.PredictedEntity

trait NLPModelImpl {

  def predict(datum: String): Set[PredictedEntity]

  def isEntity(text: String, entity: Seq[PredictedEntity]): Boolean = predict(text).exists(entity.contains)

  def createPredictions(strings: Seq[String]): Map[String, Set[PredictedEntity]]

  def checkModel(): Unit = predict("")

}
