package org.example.scanner.inference

import akka.event.slf4j.SLF4JLogging
import org.example.scanner.impl.utils.DataFrameUtils._
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.stat.KolmogorovSmirnovTest
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.example.scanner.impl.dslfunctions
import org.example.scanner.pattern.{DataPatternComplexity, QueryWithComplexity}

import scala.util.{Failure, Success, Try}
object NumberInference extends SLF4JLogging {

  private val numericTypes = Seq(ShortType, IntegerType, LongType, FloatType, DoubleType)
  private val pThreshold = 0.05

  private val toVector = udf((d: Double) => Vectors.dense(d))
  private val extractFirstElement = udf((v: org.apache.spark.ml.linalg.Vector) => v(0))

  /**
   * Performs the Kolmogorov-Smirnov goodness-of-fit test for normal distribution on a column in a DataFrame.
   *
   * @param df
   *   The DataFrame to perform the test on.
   * @param column
   *   The name of the column to perform the test on.
   * @return
   *   The p-value resulting from the test.
   */
  private def performKolmogorovSmirnovTest(df: DataFrame, column: String): Double = {
    log.info(s"Performing KS test on column $column")
    val vectorDF = df.getRandomSample(1000).withColumn("vector", toVector(col(column)))
    val scaler = new StandardScaler().setInputCol("vector").setOutputCol("scaled").setWithStd(true).setWithMean(true)
    val scalerModel = scaler.fit(vectorDF)
    val meanValue = scalerModel.mean(0)
    val stdDevValue = scalerModel.std(0)
    val scaledDF = scalerModel.transform(vectorDF)
    val scaledVectorDF = scaledDF.withColumn("scaledVector", extractFirstElement(col("scaled")))

    scaledVectorDF.show()

    println(s"Mean: $meanValue - Stddev: $stdDevValue")

    val ksTestResult = KolmogorovSmirnovTest.test(scaledVectorDF, "scaledVector", "norm", meanValue, stdDevValue)

    val pValue = ksTestResult.first().getAs[Double](0)
    log.info(f"KS test result on column $column. p-value: $pValue%.2f")
    pValue
  }

  private def performChiSquaredTest(df: DataFrame, column: String): Double = {
    log.info(s"Performing Chi-squared test on column $column")

    val castedDf = df.withColumn("castedColumn", col(column).cast("double"))

    // Perform data shifting to avoid negative values
    val minValue = castedDf.agg(min("castedColumn")).collect()(0)(0).asInstanceOf[Double]
    val shiftedDataFrame: DataFrame = castedDf.withColumn("shiftedNumericColumn", col("castedColumn") - minValue)

    val values = shiftedDataFrame
      .select(col("shiftedNumericColumn"))
      .getRandomSample(1000)
      .collect()
      .map(row => row.getDouble(0))
    val observedDenseVector = org.apache.spark.mllib.linalg.Vectors.dense(values)
    val result = Statistics.chiSqTest(observedDenseVector)

    log.info(f"Chi-squared test result on column $column. p-value: ${result.pValue % .2f}")

    result.pValue
  }

  private def performShapiroWilkTest(df: DataFrame, column: String): Double = {
    log.info(s"Performing Shapiro-Wilk test on column $column")

    val pValue = ShapiroWilkTest.test(df, column)

    log.info(f"Shapiro-Wilk test result on column $column. p-value: $pValue%.2f")

    pValue
  }

  /**
   * Calculates the mean and standard deviation for a given column in a DataFrame.
   *
   * @param df
   *   The DataFrame to calculate the summary statistics on.
   * @param column
   *   The name of the column to calculate the mean and standard deviation for.
   * @return
   *   A List containing the mean and standard deviation values in that order, or List(null, null) if any of the values
   *   is null.
   */
  private def getMeanAndStddev(df: DataFrame, column: String): (Double, Double) = {
    val vectorDF = df.withColumn("vector", toVector(col(column)))
    val scaler = new StandardScaler().setInputCol("vector").setOutputCol("scaled").setWithStd(true).setWithMean(true)
    val scalerModel = scaler.fit(vectorDF)
    (scalerModel.mean(0), scalerModel.std(0))
  }

  /**
   * Calculates the lower and upper bounds for detecting outliers using the Inter Quartile Range (IQR) method.
   *
   * @param df
   *   DataFrame containing the data.
   * @param column
   *   Name of the column for which the bounds are to be calculated.
   * @return
   *   A list of two `Double` values representing the lower and upper bounds for detecting outliers.
   */
  private def getInterQuartileRange(df: DataFrame, column: String): (Double, Double) = {
    val quartiles: Array[Double] = df.stat.approxQuantile(column, Array(0.25, 0.75), 0.0)
    val q1: Double = quartiles(0)
    val q3: Double = quartiles(1)
    val iqr: Double = q3 - q1

    // Compute lower and upper bound for outliers
    val lower: Double = q1 - 1.5 * iqr
    val upper: Double = q3 + 1.5 * iqr
    (lower, upper)
  }

  private def getMeanAndStddevWithIQR(df: DataFrame, column: String): (Double, Double) = {
    val (lowerBound, upperBound) = getInterQuartileRange(df, column)
    log.info(s"New mean and standard deviation using the Inter Quartile Range ($lowerBound to $upperBound):")
    getMeanAndStddev(df.filter(s"$column BETWEEN $lowerBound and $upperBound"), column)
  }

  private def getDistributionDSLFunction(
                                          df: DataFrame,
                                          column: String,
                                          numeric: Boolean
                                        ): Option[QueryWithComplexity] = {
    //    val pValue = performKolmogorovSmirnovTest(df, column)
    val pValue = Try(performChiSquaredTest(df, column)) match {
      case Success(pValue) => pValue
      case Failure(e) =>
        log.warn(s"Error performing the Chi-squared test on column $column", e)
        0.0
    }
    //    val pValue = performShapiroWilkTest(df, column)
    if (pValue > pThreshold) {
      val (mean, std) = getMeanAndStddevWithIQR(df, column)
      val query = Some(QueryWithComplexity(
        dslfunctions.distribution(if (numeric) dslfunctions.columnKeyword else dslfunctions.age, mean, std),
        DataPatternComplexity.NumericFunction
      ))
      log.info(s"Inferred distribution query ${query.get.query}")
      query
    } else None
  }

  def inferDistributionQuery(df: DataFrame): Seq[QueryWithComplexity] = {
    val removeWeirdCharsUDF = udf((s: String) => if (s != null) s.replaceAll("[^a-zA-Z0-9\\-/:., ]", "") else s)
    df.schema
      .flatMap {
        column =>
          log.info(s"inferDistributionQuery on column ${column.name}")
          // Check if it is a Numeric (Short, Integer, Long, Float, Double) value
          if (numericTypes.contains(column.dataType)) {
            getDistributionDSLFunction(df, column.name, numeric = true)
            // Check if it is a String or Date value
          } else if (column.dataType.equals(StringType) || column.dataType.equals(DateType)) {
            val newColumnName = "ages"
            val agesDf = df
              .withColumn("cleanedDate", removeWeirdCharsUDF(col(column.name)))
              .withColumn("date", to_date(col("cleanedDate")))
              .select("date")
              .withColumn(newColumnName, floor(datediff(current_date(), col("date")) / 365))
              .filter(s"$newColumnName >= 0 and $newColumnName < 200")
              .select(newColumnName)
              .limit(100)
            if (!agesDf.isEmpty) { getDistributionDSLFunction(agesDf, newColumnName, numeric = false) }
            else None
          } else None
      }
  }

}
