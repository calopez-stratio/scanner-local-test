package org.example.scanner.impl.evaluation

import org.example.scanner.sdk.ConfidenceEnum._
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._

private[impl] case class Contains(elements: Seq[String], caseSensitive: Boolean = true) extends EvaluationFunction {

  override def toString: String =
    s"Contains(Seq(${elements.take(3).mkString(",")},...[${elements.size}]), ${caseSensitive.toString})"

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)

  private lazy val lowerCaseElements = elements.map(_.toLowerCase)

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val datumStr = datum.asInstanceOf[String]
    if (caseSensitive) { if (elements.exists(elem => datumStr.contains(elem))) HIGH_CONFIDENCE else NO_MATCH }
    else { if (lowerCaseElements.exists(elem => datumStr.toLowerCase.contains(elem))) HIGH_CONFIDENCE else NO_MATCH }
  }

}

private[impl] object Contains extends KeywordDSL {

  override val keyword: String = "contains"

  def containsDSL(strings: Seq[String]): FunctionDSL = s"$keyword(${strings.map(e => s""""$e"""").mkString(",")})"

  def containsDSL(strings: Seq[String], caseSensitive: Boolean): FunctionDSL =
    s"$keyword(${strings.map(e => s""""$e"""").mkString(",")}, ${caseSensitive.toString})"

}
