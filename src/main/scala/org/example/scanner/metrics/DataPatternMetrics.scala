package org.example.scanner.metrics

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.example.scanner.sdk.ConfidenceEnum._

case class DataPatternMetrics(dataPatternNames: Seq[String])(implicit spark: SparkSession) extends MetricTrait {
  override val metricNames: Seq[String] = Seq.empty

  override lazy val sparkAccumulators: Map[String, LongAccumulator] = dataPatternNames
    .flatMap {
      name =>
        List(HIGH_CONFIDENCE, MEDIUM_CONFIDENCE, LOW_CONFIDENCE, NO_MATCH, IGNORE_CONFIDENCE, null).map {
          confidence => createAccumulatorName(name, confidence) -> new LongAccumulator()
        }
    }
    .toMap

  def calculateMetrics(): Unit =
    metrics = sparkAccumulators
      .filter { case (_, accumulator) => !accumulator.isZero }
      .map {
        case (name, accumulator) =>
          val (pName, confidence) = revertAccumulatorName(name)
          PatternMetric(pName, confidence, accumulator.avg.toInt, accumulator.count, accumulator.sum / 1000000)
      }
      .toSeq

  private def createAccumulatorName(name: String, confidence: Confidence): String =
    confidence match {
      case HIGH_CONFIDENCE   => s"${name}@high"
      case MEDIUM_CONFIDENCE => s"${name}@medium"
      case LOW_CONFIDENCE    => s"${name}@low"
      case NO_MATCH          => s"${name}@nomatch"
      case IGNORE_CONFIDENCE => s"${name}@ignore"
      case _                 => name
    }

  private def revertAccumulatorName(name: String): (String, String) =
    name.split('@') match {
      case Array(name, level) => (name, level)
      case _                  => (name, "total")
    }

  def addProcessingTime(name: String, nanoseconds: Long, result: Confidence): Unit = {
    sparkAccumulators.get(createAccumulatorName(name, result)).foreach(_.add(nanoseconds))
    sparkAccumulators.get(createAccumulatorName(name, null)).foreach(_.add(nanoseconds))
  }

  sparkAccumulators.foreach { case (name, accumulator) => spark.sparkContext.register(accumulator, name) }

}
