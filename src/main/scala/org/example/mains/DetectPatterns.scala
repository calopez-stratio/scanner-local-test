package org.example.mains

import org.apache.spark.sql.SparkSession
import org.example.scanner.evaluation.{Evaluator, EvaluatorMetrics}
import org.example.scanner.impl.utils.{ParseJsonUtils, SampleUtils}
import org.example.scanner.metrics.PatternMetric

import java.util.Properties


object DetectPatterns {

  private def initSparkSesion(): SparkSession = {
    SparkSession.builder()
      .appName("Spark Session")
      .config("spark.master", "local")
      .config("spark.hadoop.fs.defaultFS", "hdfs://0.0.0.0:19000")
      .config("hadoop.home.dir", "C:/hadoop/hadoop-3.3.5")
      .getOrCreate()
  }

  def main(args : Array[String]): Unit = {
    val data_patterns = Option("[{\"id\":8745,\"name\":\"Date\",\"query\":\"date() & data_type(\\\"date\\\")\"},{\"id\":8746,\"name\":\"Passport Number\",\"query\":\"passport_number() & data_type(\\\"string\\\")\"},{\"id\":8747,\"name\":\"Social Security Number\",\"query\":\"social_security_number()\"},{\"id\":8748,\"name\":\"Timestamp\",\"query\":\"timestamp()\"},{\"id\":8749,\"name\":\"Ip Address\",\"query\":\"ip_address()\"},{\"id\":8750,\"name\":\"UUID\",\"query\":\"uuid()\"},{\"id\":8751,\"name\":\"Image\",\"query\":\"image()\"},{\"id\":8752,\"name\":\"Location\",\"query\":\"location() & data_type(\\\"string\\\")\"},{\"id\":8753,\"name\":\"Document\",\"query\":\"document()\"},{\"id\":8754,\"name\":\"Email\",\"query\":\"email()\"},{\"id\":8755,\"name\":\"Driving License\",\"query\":\"driving_license(\\\"ECU\\\")\"},{\"id\":8756,\"name\":\"Country\",\"query\":\"country()\"},{\"id\":8757,\"name\":\"Gender\",\"query\":\"gender()\"},{\"id\":8758,\"name\":\"Religious Belief\",\"query\":\"religious_belief()\"},{\"id\":8759,\"name\":\"Occupation\",\"query\":\"occupation()\"},{\"id\":8761,\"name\":\"Boolean\",\"query\":\"boolean()\"},{\"id\":8762,\"name\":\"Tax Identifier\",\"query\":\"tax_identifier(\\\"ECU\\\") & data_type(\\\"string\\\")\"},{\"id\":8764,\"name\":\"Ethnicity\",\"query\":\"ethnicity()\"},{\"id\":8765,\"name\":\"IMEI\",\"query\":\"imei()\"},{\"id\":8766,\"name\":\"Date of Birth\",\"query\":\"birth_date()\"},{\"id\":8767,\"name\":\"Marital Status\",\"query\":\"marital_status()\"},{\"id\":8769,\"name\":\"Residence Permit\",\"query\":\"residence_permit()\"},{\"id\":8770,\"name\":\"Health Condition\",\"query\":\"health_condition()\"},{\"id\":8772,\"name\":\"Geolocation\",\"query\":\"geolocation()\"},{\"id\":8773,\"name\":\"Sexual Orientation\",\"query\":\"sexual_orientation()\"},{\"id\":8849,\"name\":\"Person full name\",\"query\":\"person_name() & data_type(\\\"string\\\")\"},{\"id\":8850,\"name\":\"Values\",\"query\":\"rlike(\\\"^[0-9]+[\\\\\\\\\\\\\\\\.,]?[0-9]+$\\\") & data_type(\\\"double\\\")\"},{\"id\":8901,\"name\":\"Business Name\",\"query\":\"organization() & data_type(\\\"string\\\")\"},{\"id\":9667,\"name\":\"Nationality\",\"query\":\"nationality()\"},{\"id\":9668,\"name\":\"Person Name and LastName\",\"query\":\"in_file(\\\"nombres.txt\\\"); col_name_rlike(\\\".*nombre.*\\\", soft_restrictive)\"},{\"id\":9669,\"name\":\"Person Surnames\",\"query\":\"in_file(\\\"apellidos.txt\\\"); col_name_rlike(\\\".*apellido.*\\\", soft_restrictive)\"},{\"id\":10787,\"name\":\"Codigos numericos\",\"query\":\"(rlike(\\\"^[0-9]+$\\\") & data_type(\\\"string\\\")); col_name_rlike(\\\"^.*codigo.*$\\\", check)\"},{\"id\":11281,\"name\":\"Codigos de texto\",\"query\":\"rlike(\\\"^[a-zA-Z]+$\\\"); col_name_rlike(\\\"^.*codigo.*$\\\", check)\"},{\"id\":11283,\"name\":\"Tipo Identificacion\",\"query\":\"in(\\\"REGISTRO UNICO CONTRIBUYENTE\\\", \\\"CODIGO SUPERINTENDENCIA BANCOS\\\", \\\"PASAPORTE\\\", \\\"CODIGO INTERNO\\\", \\\"CODIGO N\\\", \\\"CEDULA\\\")\"},{\"id\":11455,\"name\":\"ID Empleado\",\"query\":\"data_type(\\\"string\\\") & (id_number(\\\"ECU\\\") | passport_number() | tax_identifier(\\\"ECU\\\") | driving_license(\\\"ECU\\\"));col_name_rlike(\\\"^empleado$\\\")\"},{\"id\":11960,\"name\":\"ID Number - Numeric\",\"query\":\"data_type(\\\"long\\\") & (id_number(\\\"ECU\\\") | passport_number() | tax_identifier(\\\"ECU\\\") | driving_license(\\\"ECU\\\"))\"},{\"id\":11962,\"name\":\"ID Number - Text\",\"query\":\"data_type(\\\"string\\\") & (id_number(\\\"ECU\\\") | passport_number() | tax_identifier(\\\"ECU\\\") | driving_license(\\\"ECU\\\"))\"},{\"id\":11963,\"name\":\"Values - Text\",\"query\":\"rlike(\\\"^[0-9]+[\\\\\\\\\\\\\\\\.,][0-9]+$\\\") & data_type(\\\"string\\\")\"},{\"id\":11964,\"name\":\"Phone Number - Numeric\",\"query\":\"phone_number(\\\"ECU\\\") & data_type(\\\"long\\\")\"},{\"id\":11965,\"name\":\"Phone Number - Text\",\"query\":\"phone_number(\\\"ECU\\\") & data_type(\\\"string\\\")\"},{\"id\":11966,\"name\":\"Date - Text\",\"query\":\"date() & data_type(\\\"string\\\")\"},{\"id\":11967,\"name\":\"Account Number - Numeric\",\"query\":\"data_type(\\\"long\\\") & (account_number(\\\"ECU\\\") | rlike(\\\"(^22|21)([0-9]{8})$|(^[0-9]{8})(03|04)$\\\"))\"},{\"id\":11968,\"name\":\"Account Number - Text\",\"query\":\"data_type(\\\"string\\\") & (account_number(\\\"ECU\\\") | rlike(\\\"(^22|21)([0-9]{8})$|(^[0-9]{8})(03|04)$\\\"))\"},{\"id\":11969,\"name\":\"Credit Card - Numeric\",\"query\":\"credit_card() & data_type(\\\"long\\\")\"},{\"id\":11970,\"name\":\"Credit Card - Text\",\"query\":\"credit_card() & data_type(\\\"string\\\")\"}]")
    implicit val spark: SparkSession = initSparkSesion()
    val sample_tdm_path = "/samples/tdm"

    val user = "postgres"
    val password = "123"
    val driver = "org.postgresql.Driver"

    val connectionProperties = SampleUtils.getConnectionProperties(user,password,driver)
    val jdbcUrl = "jdbc:postgresql://localhost:5432/SparkDB"
    val tableName = "books_data"
    val table_sample_name = "tdm_samples"
    val startTime = System.nanoTime()

    val dfPostgresTable = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    dfPostgresTable.createOrReplaceTempView(tableName)

    val sampleDfList = SampleUtils.createDFWithSample(jdbcUrl,table_sample_name,connectionProperties,sample_tdm_path,tableName)
//    val dataPatterns = ParseJsonUtils.parseDataPatterns(data_patterns)
//
//    println(s"Evaluating data patterns on ${tableName}")
//    val (evaluatorMetrics, patternMetrics) = sampleDfList
//      .foldLeft((Seq.empty[EvaluatorMetrics], Seq.empty[PatternMetric])) {
//        case (accumulator, sampleDf) =>
//          sampleDf.cache()
//          val evaluator = Evaluator(sampleDf, dataPatterns)
//          val evaluatorMetrics = evaluator.evaluateDataPatternsToMetrics()
//          sampleDf.unpersist()
//          val patternMetrics = evaluator.getPatternMetrics
//          (evaluatorMetrics ++ accumulator._1, patternMetrics ++ accumulator._2)
//      }
//    evaluatorMetrics.foreach(println)
//    patternMetrics.foreach(println)
//    println(s"Evaluation finished")


    val endTime = System.nanoTime()
    val durationInMinutes = (endTime - startTime) / 1e9 / 60

    println(f"La creacion del dataframe tomó $durationInMinutes%.2f minutos")
  }

}
