package org.example.scanner.impl.model

import org.apache.spark.ml.PipelineModel

import scala.util.{Failure, Success, Try}
import com.johnsnowlabs.nlp.{Annotation, LightPipeline}
import org.example.scanner.impl.model.Prediction.PredictedEntity

case class SparkNLPModel(modelPath: String) extends NLPModelImpl {

  lazy val model: PipelineModel = initModel(modelPath)

  @transient
  private lazy val lightPipeline: LightPipeline = new LightPipeline(model)

  private def initModel(modelPath: String): PipelineModel =
    Try(PipelineModel.load(modelPath)) match {
      case Success(value)     => value
      case Failure(exception) => throw exception
    }

  private def processPrediction(prediction: Map[String, Seq[Annotation]]): Set[Prediction.Value] =
    prediction
      .get("tag")
      .filter(_.nonEmpty)
      .map(
        _.flatMap(
          e =>
            Prediction
              .findValue(e.metadata.getOrElse("entity", "0"))
              .map(
                entity =>
                  (
                    entity,
                    Try(e.metadata.getOrElse("confidence", "0.0").toDouble) match {
                      case Success(confidence) => confidence
                      case _                   => 0.0
                    }
                  )
              )
        )
      )
      .map(
        predictions =>
          if (predictions.exists(_._2 > 0.5)) Set(predictions.maxBy(_._2)._1) else Set.empty[Prediction.Value]
      )
      .getOrElse(Set.empty)

  override def predict(text: String): Set[PredictedEntity] = createPredictions(Array(text)).head._2

  override def createPredictions(strings: Seq[String]): Map[String, Set[PredictedEntity]] =
    lightPipeline
      .fullAnnotate(strings.toArray)
      .map {
        prediction =>
          val input = prediction.get("document").map(_.head.result).getOrElse("")
          val predictions = processPrediction(prediction)
          input -> predictions
      }
      .toMap

  override def checkModel(): Unit = predict("")

}
