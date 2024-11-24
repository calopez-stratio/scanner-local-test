package org.example.scanner.impl.utils

import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import NumberUtils.BigDecimalImprovements

import java.sql.{Date, DriverManager}
import java.time.{ZoneId, ZonedDateTime}
import java.util.Properties
import scala.language.postfixOps
import scala.util.Try

object SampleUtils {

  val DEFAULT_REMOVE_REGEX = "^(\\\\s*|blank|null)$"
  private val MIN_DECIMAL = 4

  // Given a percentage, returns the corresponding SQL Tablesample
  private[impl] def formatTableSample(percentage: Double): String =
    percentage match {
      case x if x >= 100.0 => ""
      case x if x == 0.0   => s"TABLESAMPLE (0.${"0" * (MIN_DECIMAL - 1)}1 PERCENT)"
      case x               => s"TABLESAMPLE ($x PERCENT)"
    }

  // Given desired sample size and table count, returns the corresponding tablesample sql expression
  private[impl] def calculateSQLTableSample(sampleSize: Int, tableCount: Long): String =
    tableCount match {
      case count if count == 0 => ""
      case count =>
        val numberPercent = BigDecimal(sampleSize * 100 / count.toDouble).roundToDouble(MIN_DECIMAL)
        formatTableSample(numberPercent)
    }

  // Given desired sample config and table count, returns the corresponding tablesample sql expression
  private[impl] def calculateSQLTableSampleWithLimits(config: SampleConfig, tableCount: Long): String = {
    val sampleSize = (config.samplePercentage * tableCount / 100) toInt
    val limitedSampleSize = sampleSize.max(config.samplePercentageAtLeast).min(config.samplePercentageAtMost)
    calculateSQLTableSample(limitedSampleSize, tableCount)
  }

  /**
   * Creates the SQL TABLESAMPLE expression given the specific config
   *
   * @param tableName
   *   the table name to extract the sample
   * @param config
   *   the sample config
   * @param column
   *   if column is defined that means its creating a column-only dataset
   * @param withRegex
   *   if true it will apply the specified sample config regex
   * @param spark
   *   the sparkSession
   * @return
   */
  private[impl] def createSQLTableSample(
                                          tableName: String,
                                          config: SampleConfig,
                                          column: Option[String] = None,
                                          withRegex: Boolean = false
                                        )(implicit spark: SparkSession): String = {
    val whereCondition = column
      .map {
        col =>
          val filter1 = s"WHERE `$col` IS NOT NULL"
          val filter2 = if (withRegex) s" AND `$col` NOT RLIKE('${config.removeNullsRegex}')" else ""
          filter1 + filter2
      }
      .getOrElse("")
    val dfCountWithFilters = spark.sql(s"SELECT COUNT(1) AS COUNT FROM $tableName $whereCondition")
    config.sampleMode match {
      case SampleMode.SAMPLE =>
        if (config.sampleForceLimit) s"LIMIT ${config.sampleSize}"
        else calculateSQLTableSample(config.sampleSize, dfCountWithFilters.first().getLong(0))
      case SampleMode.PERCENTAGE =>
        if (config.samplePercentageLimits) {
          calculateSQLTableSampleWithLimits(config, dfCountWithFilters.first().getLong(0))
        } else formatTableSample(config.samplePercentage)
    }
  }

  private def createNotNullDFs(tableName: String, numPartitions: Int, config: SampleConfig)(implicit
                                                                                            spark: SparkSession
  ): Seq[Dataset[Row]] = {
    val columns = spark.sql(s"SELECT * FROM $tableName where 1=0").columns
    columns.map {
      column =>
        val tableWithNullFilter =
          s"SELECT `$column` FROM $tableName WHERE `$column` is not null and `$column` not rlike('${config.removeNullsRegex}')"
        Try {
          val sqlTableSample = createSQLTableSample(tableName, config, Some(column), withRegex = true)
          spark.sql(s"SELECT * FROM ($tableWithNullFilter) $sqlTableSample").repartition(numPartitions)
        }.toOption
          .getOrElse {
            val filterNulls = s"SELECT $column FROM $tableName WHERE $column is not null"
            val tableSampleExpr = createSQLTableSample(tableName, config, Some(column))
            spark.sql(s"SELECT * FROM ($filterNulls) $tableSampleExpr").repartition(numPartitions)

          }
    }
  }

  /**
   * Method that creates a CSV from a DataFrame
   *
   * @param df
   *    DataFrame to be written as a CSV
   * @param sample_tdm_path
   *    Base path where the CSV will be saved
   * @param tableName
   *    Name of the table to be used in the CSV path
   * @param validatedNulls
   *    Flag to indicate if null validation was performed
   * @param numPartitions
   *    Number of partitions for the output CSV file
   */
  private def writeSample(
                           url: String,
                           sample_table_name: String,
                           connectionProperties: Properties,
                           df: DataFrame,
                           sample_tdm_path: String,
                           tableName: String,
                           validatedNulls: Boolean,
                           numPartitions: Int
                         ): Unit = {

    val conn = DriverManager.getConnection(url, connectionProperties)
    val statement = conn.createStatement()
    statement.executeUpdate(s"DELETE FROM ${sample_table_name} WHERE table_name = '$tableName'")
    statement.close()
    conn.close()

    df.write
      .mode("append")
      .jdbc(url, sample_table_name, connectionProperties)

//    df.coalesce(numPartitions)
//      .write
//      .option("header", "true")
//      .option("delimiter", ";")
//      .mode(SaveMode.Overwrite)
//      .csv(s"/tmp/$sample_tdm_path/$tableName")

  }

  private def createNotNullDFsWithSamples(
                                           url: String,
                                           sample_table_name: String,
                                           connectionProperties: Properties,
                                           sample_tdm_path: String,
                                           tableName: String, numPartitions: Int, config: SampleConfig)
                                         (implicit spark: SparkSession
                                         ): Seq[Dataset[Row]] = {
    val columns = spark.sql(s"SELECT * FROM $tableName where 1=0").columns
    var datasets: Seq[Dataset[Row]] = Seq.empty
    var datasetsWithColumn: Seq[Dataset[Row]] = Seq.empty
    val sampleDate = Date.valueOf(ZonedDateTime.now(ZoneId.of("America/Guayaquil")).toLocalDate)
    var sampleQuery = ""
    columns.foreach {
      column =>
        var df: DataFrame = null
        val tableWithNullFilter =
          s"SELECT `$column` FROM $tableName WHERE `$column` is not null and `$column` not rlike('${config.removeNullsRegex}')"
        Try {
          val sqlTableSample = createSQLTableSample(tableName, config, Some(column), withRegex = true)
          sampleQuery = s"SELECT * FROM ($tableWithNullFilter) $sqlTableSample"
          df = spark.sql(sampleQuery).repartition(numPartitions)
        }.toOption
          .getOrElse {
            val filterNulls = s"SELECT $column FROM $tableName WHERE $column is not null"
            val tableSampleExpr = createSQLTableSample(tableName, config, Some(column))
            sampleQuery = s"SELECT * FROM ($filterNulls) $tableSampleExpr"
            df = spark.sql(sampleQuery).repartition(numPartitions)
          }
        datasets :+= df
        datasetsWithColumn :+= df.withColumn("column_name", lit(column))
          .withColumn("table_name", lit(tableName))
          .withColumn("sample_date", lit(sampleDate))
          .withColumn("sample_query", lit(sampleQuery))
        println(sampleQuery)
    }
    val unionSeq: DataFrame = datasetsWithColumn.reduce(_ union _)
    val finalDf = unionSeq.withColumnRenamed(unionSeq.columns.head, "column_value")
      .select("table_name", "column_name", "column_value", "sample_query", "sample_date")
    writeSample(url,sample_table_name,connectionProperties,finalDf, sample_tdm_path, tableName, validatedNulls = true, numPartitions)
    datasets
  }

  /**
   * Validates whether a given string can represent a boolean value ("true" or "false").
   * If the string is "true" or "false" (case insensitive), it returns the same string in lowercase.
   * Otherwise, it throws an IllegalArgumentException.
   *
   * @param inputString
   *    the input string to validate as a boolean ("true" or "false")
   * @return
   *    the input string in lowercase if it represents a valid boolean
   * @throws IllegalArgumentException
   *    if the input string is not "true" or "false"
   */
  def validBoolStr(inputString: String): String = {
    val stringMinusculas = inputString.toLowerCase()
    stringMinusculas match {
      case "true" => "true"
      case "false" => "false"
      case _ =>
        throw new IllegalArgumentException("The header property is not a valid boolean: " +
          "the only supported values are true or false")
    }
  }

  def getConnectionProperties(user: String, password: String, driver: String): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", user)
    connectionProperties.setProperty("password", password)
    connectionProperties.setProperty("driver", driver)

    connectionProperties
  }

  /**
   * Method to create the dataframes given the desired sample configuration
   *
   * @param tableName
   *   name of the table in the catalog
   * @param config
   *   the desired [[SampleConfig]]
   * @param spark
   *   sparkSession as implicit
   * @return
   *   a sequence containing one dataset (if no regex is applied) or a list of datasets containing each column as single
   *   dataset with the desired regex applied.
   */
  def createDFWithSample(
                          url: String,
                          sample_table_name: String,
                          connectionProperties: Properties,
                          sample_tdm_path: String,
                          tableName: String, config: SampleConfig = SampleConfig())(implicit spark: SparkSession
                        ): Seq[Dataset[Row]] = {
    val numPartitions = config.numCores * config.partitionMultiplier
    if (config.removeNulls) createNotNullDFsWithSamples(url,sample_table_name, connectionProperties, sample_tdm_path, tableName, numPartitions, config)
    else {
      val tableSampleExpr = createSQLTableSample(tableName, config)
      val df = spark.sql(s"SELECT * FROM $tableName $tableSampleExpr").repartition(numPartitions)
      writeSample(url,sample_table_name,connectionProperties, df, sample_tdm_path, tableName, validatedNulls = false, numPartitions)
      Seq(df)
    }
  }

  /**
   * Method to create the dataframes given the desired sample configuration
   *
   * @param tableName
   *   name of the table in the catalog
   * @param config
   *   the desired [[SampleConfig]]
   * @param spark
   *   sparkSession as implicit
   * @return
   *   a sequence containing one dataset (if no regex is applied) or a list of datasets containing each column as single
   *   dataset with the desired regex applied.
   */
  def createDF(tableName: String, config: SampleConfig = SampleConfig())(implicit
                                                                         spark: SparkSession
  ): Seq[Dataset[Row]] = {
    val numPartitions = config.numCores * config.partitionMultiplier
    if (config.removeNulls) createNotNullDFs(tableName, numPartitions, config)
    else {
      val tableSampleExpr = createSQLTableSample(tableName, config)
      Seq(spark.sql(s"SELECT * FROM $tableName $tableSampleExpr").repartition(numPartitions))
    }
  }





}
