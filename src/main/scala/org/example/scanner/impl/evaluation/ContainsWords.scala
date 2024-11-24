package org.example.scanner.impl.evaluation


import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, NO_MATCH}
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._

private[impl] case class ContainsWords(elements: Seq[String], caseSensitive: Boolean = true)
  extends EvaluationFunction {

  override def toString: String =
    s"ContainsWords(Seq(${elements.take(3).mkString(",")},...[${elements.size}]), ${caseSensitive.toString})"

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)

  private lazy val elementRegexes = elements.map {
    element =>
      val caseSensitivity = if (caseSensitive) "" else "(?i)"
      s"$caseSensitivity\\b$element\\b".r
  }

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {
    val datumStr = datum.asInstanceOf[String]
    if (elementRegexes.exists(regex => regex.findFirstIn(datumStr).isDefined)) HIGH_CONFIDENCE else NO_MATCH
  }

}

private[impl] object ContainsWords extends KeywordDSL {

  override val keyword: String = "contains_words"

  def containsWordsDSL(strings: Seq[String]): FunctionDSL = s"$keyword(${strings.map(e => s""""$e"""").mkString(",")})"

  def containsWordsDSL(strings: Seq[String], caseSensitive: Boolean): FunctionDSL =
    s"$keyword(${strings.map(e => s""""$e"""").mkString(",")}, ${caseSensitive.toString})"

}
