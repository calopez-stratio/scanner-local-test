package org.example.scanner.impl.model

import akka.event.slf4j.SLF4JLogging
import org.apache.hadoop.fs.Path
import org.example.scanner.impl.model.Prediction.PredictedEntity
import org.example.scanner.impl.utils.FileUtils

case class OpenNLPModel(basePath: String) extends NLPModelImpl with SLF4JLogging {

  @transient
  private lazy val model: ONNXModel = {
    log.info(s"Loading OpenNLPModel located at: $basePath")
    val newBasePath =
      if (basePath.startsWith("hdfs://")) {
        val modelHdfsPath = new Path(s"$basePath/model.onnx")
        val vocabHdfsPath = new Path(s"$basePath/vocab.txt")

        val localPath = "/tmp"
        val modelLocalPath = s"$localPath/model.onnx"
        val vocabLocalPath = s"$localPath/vocab.txt"

        FileUtils.copyHdfsFileToLocal(modelHdfsPath, modelLocalPath)
        FileUtils.copyHdfsFileToLocal(vocabHdfsPath, vocabLocalPath)

        localPath
      } else { basePath }

    val modelPath = s"${newBasePath}/model.onnx"
    val vocabPath = s"${newBasePath}/vocab.txt"
    ONNXModel(modelPath, vocabPath, labels, entities)
  }

  private val labels = Map(
    0 -> "O",
    1 -> "B-DATE",
    2 -> "I-DATE",
    3 -> "B-PER",
    4 -> "I-PER",
    5 -> "B-ORG",
    6 -> "I-ORG",
    7 -> "B-LOC",
    8 -> "I-LOC"
  )

  private val entities = Seq(
    NerEntity(Prediction.person, "B-PER", "I-PER"),
    NerEntity(Prediction.location, "B-LOC", "I-LOC"),
    NerEntity(Prediction.organization, "B-ORG", "I-ORG")
  )

  override def createPredictions(predictions: Seq[String]): Map[String, Set[PredictedEntity]] =
    predictions.map(prediction => prediction -> predict(prediction)).toMap

  override def checkModel(): Unit = predict("")

  override def predict(datum: String): Set[PredictedEntity] =
    model
      .predict(datum)
      .flatMap(pred => Prediction.findValue(pred.predictedEntity.toString))
      .map(pred => Set(pred))
      .getOrElse(Set.empty)

}
