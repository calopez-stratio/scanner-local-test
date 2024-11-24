package org.example.scanner.inference

import org.apache.spark.sql.DataFrame

case class InferenceTarget(dataFrames: Seq[DataFrame], targetColumns: Seq[String])

object InferenceTarget {

  def apply(dataFrame: DataFrame, targetColumns: Seq[String]): InferenceTarget =
    InferenceTarget(Seq(dataFrame), targetColumns)

}