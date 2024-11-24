package org.example.scanner.metrics

import org.apache.spark.sql.SparkSession

case class ProcessingMetrics()(implicit spark: SparkSession) extends MetricTrait {

  override val metricNames: Seq[String] = Seq(
    ProcessingMetrics.PROCESS_COLUMN_METADATA,
    ProcessingMetrics.PROCESS_CALCULATE_PATTERN_ROW,
    ProcessingMetrics.PROCESS_REDUCE_PATTERNS,
    ProcessingMetrics.PROCESS_CALCULATE_CONFIDENCE
  )

  def calculateMetrics(): Unit =
    metrics = metricNames.map {
      metric =>
        PatternMetric(
          metric,
          "total",
          sparkAccumulators(metric).avg.toLong,
          sparkAccumulators(metric).count,
          sparkAccumulators(metric).sum / 1000000
        )
    }

  sparkAccumulators.foreach { case (name, accumulator) => spark.sparkContext.register(accumulator, name) }

}

object ProcessingMetrics {

  val PROCESS_COLUMN_METADATA: String = "Process_column_metadata"
  val PROCESS_CALCULATE_PATTERN_ROW: String = "Process_calculate_patterns_row"
  val PROCESS_REDUCE_PATTERNS: String = "Process_reduce_patterns"
  val PROCESS_CALCULATE_CONFIDENCE: String = "Process_calculate_confidence"
}
