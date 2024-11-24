package org.example.scanner.impl.evaluation

import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.sdk.ConfidenceEnum._
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.NoParamDSL
import org.example.scanner.sdk.types._

private[impl] case class BooleanSet() extends EvaluationFunction {

  override val supportedTypes: Seq[ScannerType] = Seq(BooleanType, StringType)

  private val booleanListHigh = BooleanSet.BooleanListHigh
  private val booleanListMed = BooleanSet.BooleanListMed

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence =
    datum match {
      case _: Boolean => HIGH_CONFIDENCE
      case d: String if d.forall(_.isLetter) =>
        d.toLowerCase match {
          case dLower if booleanListHigh.contains(dLower) => HIGH_CONFIDENCE
          case dLower if booleanListMed.contains(dLower)  => MEDIUM_CONFIDENCE
          case _                                          => NO_MATCH
        }
      case _ => NO_MATCH
    }

}

private[impl] object BooleanSet extends NoParamDSL[BooleanSet] {
  override val keyword: String = "boolean"
  private[impl] val BooleanListHighDefault: List[String] = List("true", "false", "yes", "no", "si", "oui", "non")
  private val BooleanListMedDefault: List[String] = List("t", "f", "y", "n", "s")

  private lazy val BooleanListHigh: List[String] = ScannerConfig
    .getOrElse(s"$keyword.high_confidence_list", BooleanListHighDefault)

  private lazy val BooleanListMed: List[String] = ScannerConfig
    .getOrElse(s"$keyword.medium_confidence_list", BooleanListMedDefault)

}
