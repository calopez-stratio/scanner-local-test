package org.example.scanner.evaluation

import akka.event.slf4j.SLF4JLogging
import org.example.scanner.impl.config.{ExternalJarLoader, ScannerConfig}
import org.example.scanner.impl.model.{NLPModel, NLPModelImpl}
import org.example.scanner.impl.utils.{DataFrameUtils, StructTypeUtils}
import org.example.scanner.impl.utils.StructTypeUtils.getColumnName
import org.example.scanner.metadata.MetadataMap
import org.example.scanner.metrics._
import org.example.scanner.pattern.DataPattern
import org.example.scanner.sdk.ConfidenceEnum.ConfidenceType
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.example.scanner.sdk.types.{ScannerType, ScannerTypes}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}
import org.example.scanner.evaluation.config.GlobalConfig

import scala.util.{Failure, Success, Try}


case class Evaluator(df: DataFrame, dataPatterns: Seq[DataPattern])(implicit
                                                                    spark: SparkSession,
                                                                    scannerConfig: ScannerConfig = ScannerConfig.default()
) extends SLF4JLogging {

  // Metrics variables
  private var dataPatternMetrics: DataPatternMetrics = _
  private val processingMetrics: ProcessingMetrics = ProcessingMetrics()
  private val recordsMetrics: RecordsMetrics = RecordsMetrics()
  private val modelMetrics: ModelMetrics = ModelMetrics()
  private var allMetrics: Seq[MetricTrait] = _
  private var validDataPatterns: Seq[DataPattern] = _

  /**
   * Reduces column pattern matches and column metrics for two rows.
   *
   * @param columnResultRow1
   *   a sequence of ColumnPatternMatch representing the results for the first row
   * @param columnResultRow2
   *   a sequence of ColumnPatternMatch representing the results for the second row
   * @return
   *   a sequence of ColumnPatternMatch representing the reduced results
   */
  private val reduceColumnResults =
    (columnResultRow1: Seq[ColumnPatternMatch], columnResultRow2: Seq[ColumnPatternMatch]) =>
      measureMetric(ProcessingMetrics.PROCESS_REDUCE_PATTERNS) {
        Evaluator.reduceColumnPatternMatches(columnResultRow1 ++ columnResultRow2).toSeq
      }

  def getPatternMetrics: Seq[PatternMetric] = if (null == allMetrics) Seq.empty else allMetrics.flatMap(_.metrics)

  /**
   * Evaluates data pattern metrics given the evaluation result
   *
   * @return
   *   metrics calculating the precision based on target columns
   */
  def evaluateDataPatternsToMetrics(): Seq[EvaluatorMetrics] =
    EvaluatorMetrics.getEvaluatorMetrics(evaluateDataPatterns())

  private def validateDataPatternsAndInitMetrics(): Unit = {
    validDataPatterns = dataPatterns.filter(_.testEvaluator)
    dataPatternMetrics = DataPatternMetrics(validDataPatterns.map(_.name))
    allMetrics = Seq(dataPatternMetrics, processingMetrics, recordsMetrics, modelMetrics)
  }

  /**
   * Main method of Scanner, used to scan this DataFrame with the provided Data Patterns
   *
   * @return
   *   A sequence of column pattern matches for each field.
   */
  def evaluateDataPatterns(): Seq[ColumnPatternMatch] = {
    // If df is empty, dont process anything
    if (df.head(1).isEmpty) {
      log.warn("Table is empty, could not evaluate any data pattern")
      return Seq.empty[ColumnPatternMatch]
    }

    import spark.implicits._
    log.debug(s"Table has ${df.rdd.getNumPartitions} partitions.")
    ScannerConfig.loadScannerConfig(scannerConfig) // Ensures config is initted
    validateDataPatternsAndInitMetrics()
    ExternalJarLoader.addJarsToSparkContext()

    implicit val withNLP: Boolean = validDataPatterns.exists(_.usesModel)

    // Load the model and add the model metadata extractor if any data pattern uses it
    if (withNLP) {
      log.info(s"NLP Model is needed. Number of cores per executor: ${ScannerConfig.numCoresPerExecutor}")
      Try(NLPModel.checkModel()) match {
        case Failure(e) => throw new EvaluatorException(s"Unable to check model in driver", e)
        case Success(_) => log.info("Model in driver loaded successfully")
      }
    }

    // Calculate metadata map
    val metadataMap: MetadataMap = measureMetric(ProcessingMetrics.PROCESS_COLUMN_METADATA) {
      df.mapPartitions(getMetadata)(Encoders.kryo[MetadataMap]).reduce(MetadataMap.reduce)
    }

    // Calculate data pattern matches
    val model = NLPModel.getModel
    val columnPatternMatches = df
      .mapPartitions(getPatternMatches(_, metadataMap, model))
      .reduce(reduceColumnResults)
      .map(applyColumnFunctions)
    log.info("Successfully processed data patterns on this table.")

    // Calculate metrics
    log.info("Extracting metrics on data pattern process")
    allMetrics.foreach(_.calculateMetrics())
    if (validDataPatterns.exists(_.usesModel)) {
      log.info("Cleaning NLP model predictions")
      NLPModel.clean()
    }

    // Show metrics in console. Uncomment if needed
    //    allMetrics.foreach(_.showMetrics())

    columnPatternMatches
  }

  private def evaluateDataPattern(dataPattern: DataPattern, datum: Any, isValidDatum: Boolean)(implicit
                                                                                               metadataSet: Set[ColumnMetadata],
                                                                                               scannerType: ScannerType
  ): Option[PatternMatch] = {
    val startTime = System.nanoTime()
    val maybePatternMatch = if (isValidDatum) dataPattern.evaluate(datum) else None
    val endTime = System.nanoTime()
    val duration = endTime - startTime
    dataPatternMetrics.addProcessingTime(dataPattern.name, duration, maybePatternMatch.map(_.confidences.head._1))
    maybePatternMatch
  }

  private def evaluateModel(rowList: List[Row], metadataMap: MetadataMap): Unit = {
    println(s"Evaluating model on partition")
    val startTime = System.nanoTime()
    val sizePredictions = NLPModel.createPredictions(rowList, metadataMap)
    val endTime = System.nanoTime()
    val duration = endTime - startTime
    println(s"Ended evaluating model on partition. Time elapsed [${duration / 1000000} ms]")
    if (sizePredictions > 0) {
      modelMetrics.addMetric(ModelMetrics.MODEL_PREDICT_AVG, duration / sizePredictions)
      modelMetrics.addMetric(ModelMetrics.MODEL_NUM_PREDICTIONS, sizePredictions)
    }
  }

  /**
   * Evaluates a single datum against the data patterns and returns a sequence of column pattern matches for the field
   *
   * @param fieldName
   *   The name of the field being evaluated.
   * @param datum
   *   The datum being evaluated.
   * @param metadataSet
   *   An implicit parameter representing the set of column metadata
   * @param scannerType
   *   An implicit parameter representing the Scanner type.
   * @return
   *   A sequence of column pattern matches for the field.
   */
  private def evaluateSingleDatum(fieldName: String, datum: Any)(implicit
                                                                 metadataSet: Set[ColumnMetadata],
                                                                 scannerType: ScannerType
  ): Seq[ColumnPatternMatch] = {
    val trimDatum = datum match {
      case x: String if GlobalConfig.trimEnable => x.trim
      case x                                    => x
    }
    val isValidDatum = trimDatum != null
    val detectedPatterns = validDataPatterns
      .flatMap(pattern => evaluateDataPattern(pattern, trimDatum, isValidDatum))
      .toSet
    val countMetrics = ColumnPatternMatch.createCountMetrics(isValidDatum)
    Seq(ColumnPatternMatch(fieldName, detectedPatterns, countMetrics))
  }

  private def scanMapPatterns(
                               dataMap: Map[Any, Any],
                               metadataMap: MetadataMap,
                               mapType: MapType,
                               fieldName: String
                             ): Seq[ColumnPatternMatch] = {
    val schema = StructType(Seq(StructField("key", mapType.keyType), StructField("value", mapType.valueType)))
    dataMap
      .toSeq
      .flatMap {
        case (key, value) =>
          val row = new GenericRowWithSchema(Array(key, value), schema)
          scanPatterns(row, metadataMap, Some(fieldName))
      }
  }

  private def scanPatterns(
                            row: Row,
                            metadataMap: MetadataMap,
                            parentName: Option[String] = None
                          ): Seq[ColumnPatternMatch] =
    (row.toSeq, row.schema.fields)
      .zipped
      .flatMap {
        case (datum, field) =>
          val columnName = getColumnName(parentName, field.name)
          implicit val metadataSet: Set[ColumnMetadata] = metadataMap.metadataMap.getOrElse(columnName, Set.empty)
          implicit val scannerType: ScannerType = ScannerTypes.getScannerType(field.dataType)
          (datum, field.dataType) match {
            case (dataStruct: Row, _) => scanPatterns(dataStruct, metadataMap, Some(columnName))
            case (dataArray: Seq[Any], arrayType: ArrayType) =>
              val arrayInnerType = DataFrameUtils.flattenArrayType(arrayType)
              val columnPatternMatches = StructTypeUtils
                .flattenArray(dataArray)
                .flatMap {
                  case structData: Row => scanPatterns(structData, metadataMap, Some(columnName))
                  case mapData: Map[Any, Any] =>
                    scanMapPatterns(mapData, metadataMap, arrayInnerType.asInstanceOf[MapType], columnName)
                  case arrayDatum => evaluateSingleDatum(columnName, arrayDatum)
                }
              Evaluator.reduceColumnPatternMatches(columnPatternMatches)
            case (dataMap: Map[Any, Any], mapType: MapType) => // Convert this map into a Row
              val columnPatternMatches = scanMapPatterns(dataMap, metadataMap, mapType, columnName)
              Evaluator.reduceColumnPatternMatches(columnPatternMatches)
            case _ => evaluateSingleDatum(columnName, datum)
          }
      }

  private def getMetadata(rowIter: Iterator[Row]): Iterator[MetadataMap] = {
    ScannerConfig.loadScannerConfig(scannerConfig) // Ensures config is initted
    log.debug("Processing row metadata")
    MetadataMap.processMetadata(rowIter)
  }

  private def getPatternMatches(rowIter: Iterator[Row], metadataMap: MetadataMap, model: Option[NLPModelImpl])(implicit
                                                                                                               withNLP: Boolean
  ): Iterator[Seq[ColumnPatternMatch]] = {
    ScannerConfig.loadScannerConfig(scannerConfig) // Ensures config is initted
    val rowList = rowIter.toList
    recordsMetrics.addMetric(RecordsMetrics.RECORDS_PER_PARTITION, rowList.length)
    println(s"Processing row pattern matches. Number of rows [${rowList.size}]")
    // Predict with the model if any pattern uses it
    if (withNLP) {
      println(s"NLP Model is needed. Number of cores per executor: ${ScannerConfig.numCoresPerExecutor}")
      NLPModel.checkInvalidExecutorCores()
      NLPModel.setModel(model)
      Try(NLPModel.checkModel()) match {
        case Failure(e) => throw new EvaluatorException(s"Unable to check model in executor", e)
        case Success(_) => println("Model in executor loaded successfully")
      }
      evaluateModel(rowList, metadataMap)
    }
    val rowIteratorPatternMatches = rowList
      .map(row => measureMetric(ProcessingMetrics.PROCESS_CALCULATE_PATTERN_ROW)(scanPatterns(row, metadataMap)))
      .toIterator
    println("Ending processing row pattern matches")
    rowIteratorPatternMatches
  }

  /**
   * Applies column functions to the given column result and returns a new column with updated pattern matches and
   * confidences.
   *
   * @param columnResult
   *   the column result to apply the column functions to
   * @return
   *   a new column with updated pattern matches and confidences
   */
  private def applyColumnFunctions(columnResult: ColumnPatternMatch): ColumnPatternMatch =
    measureMetric(ProcessingMetrics.PROCESS_CALCULATE_CONFIDENCE) {
      val patternMatchesAfterColumnFunction = validDataPatterns
        .flatMap {
          dataPattern =>
            // Find the matching pattern for the current column and apply its column function
            val patternMatch: Option[PatternMatch] = columnResult
              .patternMatches
              .find(patternMatch => patternMatch.dataPattern == dataPattern)
            val confidences = patternMatch.map(_.confidences)
            val newConfidences = dataPattern.evaluateColumnFunction(columnResult.colName, confidences)
            newConfidences.map(value => PatternMatch(dataPattern, value))
        }
        .toSet
      columnResult.copy(patternMatches = patternMatchesAfterColumnFunction)
    }

  /**
   * Method that measures the execution time of a given function using a specific metric.
   *
   * @param metric
   *   The name of the metric to measure.
   * @param f
   *   The function to measure.
   * @tparam A
   *   The return type of the function.
   * @return
   *   The result of the function.
   */
  private def measureMetric[A](metric: String)(f: => A): A = {
    val startTime = System.nanoTime()
    val result = f
    val totalTime = System.nanoTime() - startTime
    processingMetrics.addMetric(metric, totalTime)
    result
  }

}

object Evaluator {

  def apply(df: DataFrame, dataPattern: DataPattern)(implicit spark: SparkSession): Evaluator =
    Evaluator(df, Seq(dataPattern))

  private[evaluation] def reduceColumnPatternMatches(
                                                      columnPatternMatches: Seq[ColumnPatternMatch]
                                                    ): Iterable[ColumnPatternMatch] =
    columnPatternMatches
      .groupBy(_.colName)
      .map {
        case (colName: String, columnPatternMatches: Seq[ColumnPatternMatch]) =>
          val patternMatches = columnPatternMatches.flatMap(_.patternMatches.toSeq)
          val reducedPatternMatches = reducePatternMatches(patternMatches).toSet
          ColumnPatternMatch(
            colName,
            reducedPatternMatches,
            ColumnPatternMatch.combineMetrics(columnPatternMatches.map(_.columnMetrics))
          )
      }

  private[evaluation] def reducePatternMatches(patternMatches: Seq[PatternMatch]): Iterable[PatternMatch] =
    patternMatches
      .groupBy(_.dataPattern)
      .map {
        case (dataPattern: DataPattern, patternMatches: Seq[PatternMatch]) =>
          val confidencesMaps = patternMatches.map(_.confidences)
          val reducedConfidencesMap = reduceConfidencesMaps(confidencesMaps)
          PatternMatch(dataPattern, reducedConfidencesMap)
      }

  private[evaluation] def reduceConfidencesMaps(
                                                 confidencesMaps: Seq[Map[ConfidenceType, Int]]
                                               ): Map[ConfidenceType, Int] =
    confidencesMaps
      .reduce((map1, map2) => map1.foldLeft(map2) { case (map, (k, v)) => map + (k -> (v + map.getOrElse(k, 0))) })

}

private class EvaluatorException(msg: String, thr: Throwable) extends Exception(msg, thr)
