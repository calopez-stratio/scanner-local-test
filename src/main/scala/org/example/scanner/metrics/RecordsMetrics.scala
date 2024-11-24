package org.example.scanner.metrics

import org.apache.spark.sql.SparkSession

case class RecordsMetrics(implicit spark: SparkSession) extends MetricTrait {

  override val metricNames: Seq[String] = Seq(RecordsMetrics.RECORDS_PER_PARTITION)

  override def calculateMetrics(): Unit =
    metrics = metricNames.map {
      metric =>
        PatternMetric(
          metric,
          "total",
          sparkAccumulators(metric).avg.toLong,
          sparkAccumulators(metric).sum,
          sparkAccumulators(metric).count
        )
    }

  sparkAccumulators.foreach { case (name, accumulator) => spark.sparkContext.register(accumulator, name) }

}

object RecordsMetrics {
  val RECORDS_PER_PARTITION: String = "Records_partition"
}
