package org.example.scanner.metrics

import org.apache.spark.sql.SparkSession

case class ModelMetrics()(implicit spark: SparkSession) extends MetricTrait {

  override val metricNames: Seq[String] = Seq(ModelMetrics.MODEL_PREDICT_AVG, ModelMetrics.MODEL_NUM_PREDICTIONS)

  def calculateMetrics(): Unit =
    metrics = Seq(PatternMetric(
      ModelMetrics.MODEL_PREDICT_AVG,
      "total",
      sparkAccumulators(ModelMetrics.MODEL_PREDICT_AVG).avg.toLong,
      sparkAccumulators(ModelMetrics.MODEL_NUM_PREDICTIONS).sum,
      sparkAccumulators(ModelMetrics.MODEL_PREDICT_AVG).avg.toLong / 1000000 *
        sparkAccumulators(ModelMetrics.MODEL_NUM_PREDICTIONS).sum
    ))

  sparkAccumulators.foreach { case (name, accumulator) => spark.sparkContext.register(accumulator, name) }

}

object ModelMetrics {

  val MODEL_PREDICT_AVG: String = "NLP Model"
  val MODEL_NUM_PREDICTIONS: String = "Num_Predictions"
}
