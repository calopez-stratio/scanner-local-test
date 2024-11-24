package org.example.scanner.impl.evaluation

import org.apache.commons.math3.distribution.{NormalDistribution => MathNormalDistribution}
import org.example.scanner.dsl.ast.TransformationFunction
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.sdk.ConfidenceEnum._
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._

import java.util.Locale

private[impl] case class InlierNormalDist(func: TransformationFunction, mean: Double, std: Double)
  extends EvaluationFunction {

  override val supportedTypes: Seq[ScannerType] = ScannerTypes.NUMERIC_TYPES

  override def isCompatible(implicit datumType: ScannerType): Boolean =
    func.isCompatibleForParentTypes(parentTypes = supportedTypes)

  private val highProbValue = InlierNormalDist.highProbValue
  private val mediumProbValue = InlierNormalDist.mediumProbValue
  private val lowProbValue = InlierNormalDist.lowProbValue

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence =
    func.evaluate(datum: Any) match {
      case evaluationResults: Seq[_] =>
        var bestConfidence = NONE
        evaluationResults
          .asInstanceOf[Seq[BigDecimal]]
          .takeWhile {
            result =>
              val res = checkDistribution(result)
              if (res.isDefined && (res.get.id > bestConfidence.id)) bestConfidence = res.get
              res.isEmpty || (res.isDefined && res.get.id != 3)
          }
        if (bestConfidence != NONE) Some(bestConfidence) else NO_MATCH
      case valueEv: BigDecimal => checkDistribution(valueEv)
      case _                   => NO_MATCH
    }

  // TODO: probar K-S (99, 95, 90, <90) <95 no suele considerarse
  private def checkDistribution(datum: BigDecimal): Confidence = {
    val z = if (std > 0) (datum - mean).abs / std else return NO_MATCH
    // hasta 2 es medio-normal, a partir de 3 .......... :)
    //    [1,2,4,2,4,5]
    //    [LOW, MEDIUM, HIGH, LOW, NONE, NONE] -> 40% normal_distribution
    // observacion -> [HIGH->10,Medium->50, LOW->100, NONE-> 20]
    // Normal ->      [HIGH->30,Medium->60, LOW->90 , NONE-> 30] --> EVALUATION (ANOVA) --> HIGH, MEDIUM, LOW, NONE

    if (z <= highProbValue) { HIGH_CONFIDENCE }
    else if (z <= mediumProbValue) { MEDIUM_CONFIDENCE }
    else if (z <= lowProbValue) { LOW_CONFIDENCE }
    else { NO_MATCH }
  }

}

private[impl] object InlierNormalDist extends KeywordDSL {

  Locale.setDefault(Locale.US)

  override val keyword: String = "inlier_normal_dist"

  private lazy val normal: MathNormalDistribution = new MathNormalDistribution(0, 1)
  private val highProbValueDefault: Double = 2
  private val mediumProbValueDefault: Double = 3
  private val lowProbValueDefault: Double = 4

  private lazy val highProbValue: Double = ScannerConfig.getOrElse(s"$keyword.high_prob_value", highProbValueDefault)

  private lazy val mediumProbValue: Double = ScannerConfig
    .getOrElse(s"$keyword.medium_prob_value", mediumProbValueDefault)

  private lazy val lowProbValue: Double = ScannerConfig.getOrElse(s"$keyword.low_prob_value", lowProbValueDefault)

  def distributionDSL(func: FunctionDSL, mean: Double, std: Double): FunctionDSL =
    s"$keyword($func, ${"%.3f".format(mean)}, ${"%.3f".format(std)})"

  def distribution(func: TransformationFunction, mean: Double, std: Double): InlierNormalDist =
    InlierNormalDist(func, mean, std)

}
