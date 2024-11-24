package org.example.scanner.impl.evaluation

import org.example.scanner.metadata.{ColumnMetadata, DateFormatMetadata}
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._

import java.text.SimpleDateFormat
import scala.util.{Failure, Success, Try}

private[impl] case class Castable(dataType: ScannerType) extends EvaluationFunction {

  private lazy val timestampDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val strDatum = datum.toString
    Try(dataType match {
      case ShortType     => strDatum.toShort
      case IntegerType   => strDatum.toInt
      case LongType      => strDatum.toLong
      case FloatType     => strDatum.toFloat
      case DoubleType    => strDatum.toDouble
      case NumericType   => BigDecimal(strDatum)
      case StringType    => datum
      case BooleanType   => strDatum.toBoolean
      case TimestampType => timestampDateFormat.parse(strDatum)
      case DateType =>
        val dateFormats =
          ColumnMetadata.findMetadata[DateFormatMetadata].map(_.dateFormats).getOrElse(Set.empty[String])
        val successfulParses = dateFormats.flatMap {
          dateFormat =>
            Try {
              val dateFormatter = new SimpleDateFormat(dateFormat)
              dateFormatter.parse(strDatum)
            }.toOption
        }
        if (successfulParses.isEmpty) throw new Exception("The datum could not be parsed to a date") else datum
      case _ => throw new Exception("Unknown data type received in castable function")
    }) match {
      case Success(_) => HIGH_CONFIDENCE
      case Failure(_) => NO_MATCH
    }
  }

}

private[impl] object Castable extends KeywordDSL {

  override val keyword: String = "castable"

  def castabaleDSL(dataType: ScannerType): FunctionDSL = s"$keyword(${s""""${dataType.name}""""})"

}
