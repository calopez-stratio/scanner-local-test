package org.example.scanner.inference


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.scanner.evaluation.{Evaluator, EvaluatorMetrics}
import org.example.scanner.impl.config.{ExternalJarLoader, ScannerConfig}
import org.example.scanner.pattern.DataPattern
object InferEngine {

  private val InferenceColumnStart: String = "scanner_inference"
  private val NumRandomChars: Int = 5
  private val MinInferenceConfidence: Double = 10.0d // TODO: Config

  /**
   * Includes a random alphanumeric prefix to the column names of a DataFrame.
   *
   * @param inferenceTarget
   *   The target DataFrame and its columns to be transformed.
   * @return
   *   A new InferenceTarget instance with the modified DataFrame and updated column names.
   */
  private def includeDFPrefix(inferenceTarget: InferenceTarget): InferenceTarget =
    inferenceTarget match {
      case InferenceTarget(dataFrames, targetColumns) =>
        val prefix = InferenceColumnStart + scala.util.Random.alphanumeric.take(NumRandomChars).mkString.toLowerCase()
        val renamedColumnsDataFrames = dataFrames
          .map(df => df.columns.foldLeft(df)((tempDf, colName) => tempDf.withColumnRenamed(colName, prefix + colName)))
        InferenceTarget(renamedColumnsDataFrames, targetColumns.map(s => s"$prefix$s"))
    }

  /**
   * Removes the prefix on the given column name.
   *
   * @param col
   *   The target column to remove the prefix.
   * @return
   *   The updated column name.
   */
  def removeDFPrefix(col: String): String =
    if (col.startsWith(InferenceColumnStart)) col.substring(InferenceColumnStart.size + NumRandomChars) else col

  /**
   * Evaluates the data patterns on the DataFrames and computes the precision metrics using the target columns as true
   * positives
   *
   * @param dataFrames
   *   DataFrames to evaluate the DataPatterns on
   * @param dataPatterns
   *   DataPatterns that will be evaluated
   * @param targetColumns
   *   target columns defined as true positives
   * @return
   *   metrics calculating the precision based on target columns
   */
  private def evaluateDataPatternPrecision(
                                            dataFrames: Seq[DataFrame],
                                            dataPatterns: Seq[DataPattern],
                                            targetColumns: Seq[String]
                                          )(implicit spark: SparkSession, scannerConfig: ScannerConfig): Seq[EvaluatorMetrics] =
    EvaluatorMetrics.getEvaluatorMetrics(
      dataFrames.flatMap(df => Evaluator(df, dataPatterns).evaluateDataPatterns()),
      Some(targetColumns)
    )

  /**
   * Obtains the evaluation metrics of the best inferred data patterns on the given inference targets.
   *
   * @param inferenceTargetsInput
   *   A list of inference target containing the dataframe and columns to infer
   * @return
   *   The evaluation metrics of the inference.
   */
  def inferDataPatternMetrics(
                               inferenceTargetsInput: Seq[InferenceTarget]
                             )(implicit spark: SparkSession, scannerConfig: ScannerConfig = ScannerConfig.default()): Seq[EvaluatorMetrics] = {
    ScannerConfig.loadScannerConfig(scannerConfig)
    ExternalJarLoader.addJarsToSparkContext()
    // Adds a prefix to each column in each dataframe
    val inferenceTargets = inferenceTargetsInput.map(includeDFPrefix)
    // Persist all dataframes
    inferenceTargets.foreach(_.dataFrames.foreach(_.persist()))
    // Calculate individual inferred data patterns
    val evaluatorMetrics = inferenceTargets
      .flatMap {
        case InferenceTarget(dataFrames, targetColumns) =>
          val inferredDataPatterns = DataPatternInference.inferDataPatterns(dataFrames, targetColumns)
          evaluateDataPatternPrecision(dataFrames, inferredDataPatterns, targetColumns)
      }
      .filter(_.confidence >= MinInferenceConfidence)
    val allTargetColumns = inferenceTargets.flatMap(_.targetColumns)
    // Generate combination of queries based on metrics
    val queriesWithComplexity = DataPatternInference.generateQueryCombinations(evaluatorMetrics, allTargetColumns)
    val combinedDataPatterns = DataPatternInference.combinedQueriesToDataPatterns(queriesWithComplexity)
    // Calculate combined inferred data patterns
    val combinedEvaluatorMetrics = inferenceTargets.flatMap {
      case InferenceTarget(dataFrames, targetColumns) =>
        evaluateDataPatternPrecision(dataFrames, combinedDataPatterns, targetColumns)
    }
    // Unpersist all dataframes
    inferenceTargets.foreach(_.dataFrames.foreach(_.unpersist()))
    // Filter metrics that dont contain target column and combine the metrics
    (evaluatorMetrics ++ combinedEvaluatorMetrics)
      .filter(c => allTargetColumns.contains(c.columnName))
      .groupBy(_.dataPattern.name)
      .mapValues {
        metrics =>
          val confidence = metrics.map(_.confidence).sum / allTargetColumns.length
          val numMatches = metrics.map(_.numMatches).sum
          val numNulls = metrics.map(_.numNulls).sum
          val numRows = metrics.map(_.numRows).sum
          val precision = metrics.map(_.precision).sum / allTargetColumns.length
          EvaluatorMetrics(
            dataPattern = metrics.head.dataPattern,
            columnName = allTargetColumns.map(removeDFPrefix).mkString(" & "),
            confidenceLevel = EvaluatorMetrics.getConfidenceValue(confidence).toString,
            confidence = confidence,
            precision = precision,
            percentageMatches = numMatches * 100 / numRows,
            numMatches = numMatches,
            numNulls = numNulls,
            numRows = numRows
          )
      }
      .values
      .toSeq
  }

}
