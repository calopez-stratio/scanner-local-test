package org.example.scanner.metrics

import org.apache.spark.util.LongAccumulator

trait MetricTrait {

  val metricNames: Seq[String]
  var metrics: Seq[PatternMetric] = Seq.empty[PatternMetric]

  lazy val sparkAccumulators: Map[String, LongAccumulator] = metricNames
    .map(name => name -> new LongAccumulator())
    .toMap

  def calculateMetrics(): Unit

  def showMetrics(): Unit = {
    if (metrics == null) calculateMetrics()
    metrics.foreach {
      case PatternMetric(name, confidence, nanoseconds, records, total_processing_time) =>
        println(f"[$name] [$confidence] [$nanoseconds ns avg] [$records num_records] [$total_processing_time ms total]")
    }
  }

  def addMetric(patternName: String, nanoseconds: Long): Unit =
    sparkAccumulators.get(patternName).foreach(_.add(nanoseconds))

}
