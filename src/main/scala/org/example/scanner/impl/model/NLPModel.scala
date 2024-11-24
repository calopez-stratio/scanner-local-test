package org.example.scanner.impl.model

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.sql.Row
import org.example.scanner.evaluation.config.GlobalConfig
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.model.Prediction.PredictedEntity
import org.example.scanner.impl.utils.{RowData, RowUtils}
import org.example.scanner.inference.InferEngine
import org.example.scanner.metadata.{ColumnMetadata, ColumnNameMetadata, MetadataMap}
import org.example.scanner.sdk.traits.dsl.ColumnMetadata

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

object NLPModel extends SLF4JLogging {

  private val predictions: mutable.HashMap[String, Set[PredictedEntity]] = mutable.HashMap.empty
  private val ignoredDatums: mutable.HashMap[Int, mutable.Set[String]] = mutable.HashMap.empty
  private val modelClasses: Seq[String => NLPModelImpl] = Seq(OpenNLPModel, SparkNLPModel)

  private lazy val localResourcePath = getClass.getResource(".").toString.split("target").head.replace("file:", "")
  private lazy val localOpenNLPModelPath: String = localResourcePath + "src/main/resources/models/opennlp_model"

  private var model: Option[NLPModelImpl] = None

  private val baseConfigKey = "nlp_model"

  /** Model configuration parameters */
  lazy val minDistinctElementsPerColumn: Int = ScannerConfig
    .getOrElse(s"$baseConfigKey.min_distinct_elements_per_column", 5)

  private lazy val minStringNlpModel: Int = ScannerConfig.getOrElse(s"$baseConfigKey.min_text_length", 3)
  private lazy val maxStringNlpModel: Int = ScannerConfig.getOrElse(s"$baseConfigKey.max_text_length", 95)
  private lazy val colNameEnable: Boolean = ScannerConfig.getOrElse(s"$baseConfigKey.col_name_enable", false)
  private lazy val colNameSeparator: String = ScannerConfig.getOrElse(s"$baseConfigKey.col_name_separator", ": ")

  private lazy val lower_input: Boolean = ScannerConfig.getOrElse(s"$baseConfigKey.lower_input", false)

  lazy val sampleSizePerColumnPartition: Int = ScannerConfig
    .getOrElse(s"$baseConfigKey.sample_size_column_partition", 100)

  // Ignore if: 6 contiguous numbers or more &&  Does not contain any letter
  private lazy val ignoreRegexList: Seq[Regex] = ScannerConfig
    .getOrElse(s"$baseConfigKey.ignore_regex_list", Seq("[0-9]{6}[0-9]*", "^[^A-Za-z]*$"))
    .map(_.r)

  def checkInvalidExecutorCores(): Unit =
    if (ScannerConfig.numCoresPerExecutor != 1 && model.exists(_.isInstanceOf[SparkNLPModel]))
      throw new InvalidCoresException

  def setModel(model: Option[NLPModelImpl]): Unit = if (this.model.isEmpty) this.model = model

  def getModel: Option[NLPModelImpl] = this.model

  def checkModel(): Unit = {
    if (model.isEmpty) {
      log.info("Model is null. Loading model...")
      loadModel()
    }
    assert(model.isDefined, "Model is not loaded after trying to load it")
    log.info(s"Checking model ${model.get.getClass.getSimpleName}...")
    model.get.checkModel()
  }

  def setSparkNLPModel(): Unit = {
    ScannerConfig.isInit = false
    ScannerConfig
      .loadScannerConfig(ScannerConfig.withSparkNLP(localResourcePath + "src/main/resources/models/sparknlp_model"))
  }

  private def loadModel(): Unit = {
    log.info(s"Loading model from path: ${ScannerConfig.modelPath}")
    val modelList = modelClasses.foldLeft(Seq.empty[NLPModelImpl]) {
      case (modelList, modelClass) =>
        if (modelList.nonEmpty) modelList
        else {
          Try {
            val modelPathToLoad = ScannerConfig.modelPath.getOrElse(localOpenNLPModelPath)
            val loadedModel = modelClass(modelPathToLoad)
            loadedModel.checkModel()
            loadedModel
          } match {
            case Success(value) =>
              log.info(s"The loaded model is of type ${modelClass.getClass.getSimpleName}")
              Seq(value)
            case Failure(e) =>
              log.warn(s"Unable to load model [${modelClass.getClass.getSimpleName}]", e)
              modelList
          }
        }
    }
    val loadedModel = Try(modelList.head) match {
      case Failure(e) =>
        throw new ModelNotFoundException(s"No models could be loaded from path ${ScannerConfig.modelPath}", e)
      case Success(model) => model
    }
    model = Some(loadedModel)
  }

  /**
   * Checks if the datum could be a Model Entity according to some filters.
   *
   * @param datum
   *   The datum to evaluate.
   * @return
   *   Whether the datum passes the filters or not.
   */
  private def isValidDatumForModel(datum: String): Boolean =
    datum.length >= minStringNlpModel && datum.length <= maxStringNlpModel &&
      ignoreRegexList.forall(_.findFirstIn(datum).isEmpty)

  def createPredictions(rowList: List[Row], metadataMap: MetadataMap): Int = {
    val rowDataList = RowUtils.getRowListData(rowList)
    println(s"Generating prediction for this row for [${rowList.size}] elements")
    val dataToPredict = rowDataList
      .groupBy(_.columnName)
      .map {
        case (columnName: String, rowData: List[RowData]) => rowData
          .map(if (GlobalConfig.trimEnable) _.datum.trim else _.datum)
          .distinct
          .view
          .filter(isValidDatumForModel)
          .map(datum => addColumnAndNormalize(datum, columnName))
          .filterNot(predictions.contains)
          .take(sampleSizePerColumnPartition)
          .toList
      }
      .fold(List.empty[String])((d1, d2) => (d1 ++ d2).distinct)
    val newPredictions = model.get.createPredictions(dataToPredict)
    predictions ++= newPredictions
    println(s"New [${newPredictions.size}] predictions  stored. Number of stored predictions [${predictions.size}]")
    newPredictions.size
  }

  def clean(): Unit = predictions.clear()

  /**
   * Retrieves the prediction for a given datum. It uses the hashcode of the calling class so it can track each
   * individual ignore list.
   *
   * @param datum
   *   The datum to be predicted.
   * @param functionId
   *   Hashcode identifier for the prediction function.
   * @param metadataSet
   *   Set of column metadata.
   * @return
   *   A tuple containing the predicted entities and a flag indicating if the datum was ignored.
   */
  def getPrediction(datum: String, functionId: Int)(implicit
                                                    metadataSet: Set[ColumnMetadata]
  ): Option[Set[PredictedEntity]] = {
    lazy val columnName = ColumnMetadata.findMetadata[ColumnNameMetadata].map(_.columnName).getOrElse("")
    lazy val preparedDatum = addColumnAndNormalize(datum, columnName)
    if (!isValidDatumForModel(datum)) Some(Set.empty[PredictedEntity]) else { predictions.get(preparedDatum) }
  }

  // Transforms the datum to lowercase if the lower_input config value is set to true
  private def normalizeDatum(datum: String): String = if (lower_input) datum.toLowerCase else datum

  // Adds column name with separator if enabled
  private def addColumnAndNormalize(datum: String, colName: String): String = {
    val normalized = normalizeDatum(datum)
    if (colNameEnable && colName.nonEmpty) InferEngine.removeDFPrefix(colName) + colNameSeparator + normalized
    else normalized
  }

  def createPredictionsForTesting(strings: Seq[String]): Unit = {
    checkModel()
    predictions ++= model.get.createPredictions(strings)
  }

}

private class ModelNotFoundException(msg: String, thr: Throwable) extends Exception(msg, thr)

private class InvalidCoresException extends Exception(
  "Number of cores per executor > 1, predictions are not in batch and will result in a MUCH SLOWER PROCESS (> x10)." +
    " You must change Spark Settings in this workflow to allow only 1 core per executor."
)
