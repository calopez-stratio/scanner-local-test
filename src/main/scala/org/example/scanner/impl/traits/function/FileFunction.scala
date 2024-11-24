package org.example.scanner.impl.traits.function

import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.types.ScannerType

import java.util.Base64
import scala.collection.mutable
import scala.util.{Success, Try}

private[impl] trait FileFunction extends EvaluationFunction {

  val signaturesHex: Seq[String]

  private lazy val minLengthHex: Int = signaturesHex.map(_.length).min

  private def genericFileEvaluation(datum: Any): Confidence = {
    val datumArrayBytes: Option[String] = datum match {
      case datum: String if checkBase64(datum) =>
        Try(Base64.getDecoder.decode(new String(datum).getBytes("UTF-8"))) match {
          case Success(arrBytes: Array[Byte]) => Some(convertBytesToHexFormat(arrBytes))
          case _                              => None
        }
      case datum: Array[Byte] => Some(convertBytesToHexFormat(datum))
      case _                  => None
    }

    datumArrayBytes match {
      case Some(datumHex: String) =>
        if (
          datumHex.length >= minLengthHex && signaturesHex.exists(
            signature =>
              datumHex.length >= signature.length && datumHex.substring(0, signature.length).contains(signature)
          )
        ) HIGH_CONFIDENCE
        else NO_MATCH
      case None => NO_MATCH
    }
  }

  private def checkBase64(datum: String): Boolean =
    datum.length >= 4 && datum.length % 4 == 0 &&
      datum.substring(0, datum.length - 3).forall(x => x.isLetterOrDigit || x == '+' || x == '/') &&
      datum.substring(datum.length - 3, datum.length).forall(x => x.isLetterOrDigit || x == '+' || x == '/' || x == '=')

  private def convertBytesToHexFormat(bytes: Array[Byte]): String = {
    val sb = new mutable.StringBuilder
    for (b <- bytes) sb.append(String.format("%02x", Byte.box(b)))
    sb.toString.toUpperCase
  }

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence =
    genericFileEvaluation(datum)

}
