package org.example.scanner.inference

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.stat.StatUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col}

object ShapiroWilkTest {

  /**
   * Performs the Shapiro-Wilk Test for Normality
   *
   * @param df
   *   DataFrame - the dataframe that contains the data
   * @param column
   *   String - the name of the column for which the test will be performed
   * @return
   *   Double - the p-value from the test. A small p-value (typically ≤ 0.05) indicates strong evidence against the null
   *   hypothesis, so you reject the null hypothesis.
   */
  def test(df: DataFrame, column: String): Double = {
    val n = df.count().toInt
    val sortedData = df.select(col(column).cast("double")).sort(asc(column)).rdd.map(row => row.getDouble(0)).collect()

    val mu = StatUtils.mean(sortedData)
    val sigma = math.sqrt(StatUtils.variance(sortedData))

    val zScores = sortedData.map(value => (value - mu) / sigma)
    val w = zScores
      .indices
      .map {
        i =>
          val a = zScores(i) * math.sqrt((i + 1.0) / n)
          val b = zScores(n - 1 - i) * math.sqrt(i.toDouble / n)
          a + b
      }
      .max

    val normalDistribution = new NormalDistribution(0, 1)
    val pValue = 1.0 - normalDistribution.cumulativeProbability(w)

    pValue
  }

}
