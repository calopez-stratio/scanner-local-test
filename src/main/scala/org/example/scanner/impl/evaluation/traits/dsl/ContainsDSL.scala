package org.example.scanner.impl.evaluation.traits.dsl



import org.example.scanner.impl.evaluation._
import org.example.scanner.impl.utils.FileUtils
import org.example.scanner.sdk.traits.dsl.EvaluationFunction
import org.example.scanner.sdk.traits.impl.{FunctionDSL, NoParamDSL}

import scala.collection.immutable.HashSet

private[impl] trait ContainsDSL extends NoParamDSL[In] {

  lazy val setOfElements: HashSet[String] = FileUtils.getIsInResource(keyword)

  def levenshteinDSL(threshold: Double): FunctionDSL = s"$keyword($threshold)"

  override def apply(): In = In.apply(setOfElements, None)

  override def noParamFunction: In = levenshteinFunction(None)

  def levenshteinFunction(maybeThreshold: Option[Double]): In = In.apply(setOfElements, maybeThreshold)

}

object ContainsDSL {

  private val thresholdFunctions: Map[String, Option[Double] => EvaluationFunction] = Map(
    Country.keyword -> Country.levenshteinFunction,
    Ethnicity.keyword -> Ethnicity.levenshteinFunction,
    MaritalStatus.keyword -> MaritalStatus.levenshteinFunction,
    Nationality.keyword -> Nationality.levenshteinFunction,
    HealthCondition.keyword -> HealthCondition.levenshteinFunction,
    SexualOrientation.keyword -> SexualOrientation.levenshteinFunction,
    ReligiousBelief.keyword -> ReligiousBelief.levenshteinFunction,
    Gender.keyword -> Gender.levenshteinFunction,
    Occupation.keyword -> Occupation.levenshteinFunction,
    ZipCode.keyword -> ZipCode.levenshteinFunction,
    ISO2CountryCodes.keyword -> ISO2CountryCodes.levenshteinFunction,
    ISO3CountryCodes.keyword -> ISO3CountryCodes.levenshteinFunction
  )

  def getDatumFunction(key: String, maybeThreshold: Option[Double]): EvaluationFunction =
    thresholdFunctions(key)(maybeThreshold)

  def isKeyDefined(key: String): Boolean = thresholdFunctions.contains(key)
}
