package org.example.mains

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.{Files, Paths}
import java.util.Properties
import scala.jdk.CollectionConverters.asScalaIteratorConverter

object CreateDataMock {

  private def initSparkSesion(): SparkSession = {
    SparkSession.builder()
      .appName("Ejemplo de Spark con Scala")
      .config("spark.master", "local")
      .getOrCreate()
  }

  private def getPostgresConnection:  Properties = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "postgres")
    connectionProperties.setProperty("password", "123")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    connectionProperties
  }

  private def createDataMock(spark: SparkSession): Unit = {
    val jdbcUrl = "jdbc:postgresql://localhost:5432/SparkDB"
    val connectionProperties = getPostgresConnection
    val csvDirectory = "src/main/resources"

    val csvFiles = Files.list(Paths.get(csvDirectory)).iterator().asScala
      .filter(path => path.toString.endsWith(".csv"))
      .toList

    csvFiles.foreach { csvFile =>
      val tableName = csvFile.getFileName.toString.replace(".csv", "")

      println("Start of ingestion for table: " +  tableName)

      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvFile.toString)

      df.write
        .mode(SaveMode.Overwrite)
        .jdbc(jdbcUrl, tableName, connectionProperties)

      println("End of ingestion for table: " +  tableName)
    }


  }

  def main(args : Array[String]): Unit = {
    val spark = initSparkSesion()

    createDataMock(spark)
  }

}
