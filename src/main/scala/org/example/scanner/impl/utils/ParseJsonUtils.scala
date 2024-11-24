package org.example.scanner.impl.utils


import org.example.scanner.inference.{InferenceTableTarget, InfereneceDataPatternTarget}
import org.example.scanner.pattern.{DataPattern, DetectionDataPattern, DetectionTable}
import spray.json._

import scala.util.{Failure, Success, Try}
object ParseJsonUtils extends DefaultJsonProtocol {

  private val QuoteString: String = "&quot;"
  implicit val dataPatternDecoder: RootJsonFormat[DetectionDataPattern] = jsonFormat3(DetectionDataPattern)
  implicit val targetTableFormat: RootJsonFormat[InferenceTableTarget] = jsonFormat2(InferenceTableTarget)
  implicit val inferenceDecoder: RootJsonFormat[InfereneceDataPatternTarget] = jsonFormat2(InfereneceDataPatternTarget)

  def parseInferenceTargets(stringMap: Option[String]): List[InfereneceDataPatternTarget] =
    Try {
      val inferenceTargetData = stringMap.get.replace(QuoteString, "\"")
      inferenceTargetData.parseJson.convertTo[List[InfereneceDataPatternTarget]]
    } match {
      case Failure(exception) => throw new ParseJsonException("InferenceTarget", exception)
      case Success(value)     => value
    }

  def parseDataPatterns(dataPatternsString: Option[String]): List[DataPattern] =
    Try {
      val dataPatternsJson = dataPatternsString.get.replace(QuoteString, "\"")
      dataPatternsJson
        .parseJson
        .convertTo[List[DetectionDataPattern]]
        .map(
          dataPatternParam =>
            DataPattern(id = dataPatternParam.id, name = dataPatternParam.name, query = dataPatternParam.query)
        )
    } match {
      case Failure(exception) => throw new ParseJsonException("DataPatterns", exception)
      case Success(value)     => value
    }

  def parseTables(tablesString: Option[String]): List[DetectionTable] =
    Try {
      val dataPatternsJson = tablesString.get.replace(QuoteString, "\"")
      dataPatternsJson.parseJson.convertTo[List[String]].map(DetectionTable.apply)
    } match {
      case Failure(exception) => throw new ParseJsonException("Tables", exception)
      case Success(value)     => value
    }

}

private class ParseJsonException(msg: String, thr: Throwable) extends Exception(s"Unable to parse [$msg]", thr)
