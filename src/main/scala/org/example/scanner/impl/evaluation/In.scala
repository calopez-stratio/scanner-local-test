package org.example.scanner.impl.evaluation

import com.github.vickumar1981.stringdistance.StringDistance.Levenshtein
import org.example.scanner.sdk.ConfidenceEnum._
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, EvaluationFunction}
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}
import org.example.scanner.sdk.types._

import scala.collection.immutable.HashSet
private[impl] case class In(setOfElements: HashSet[String], threshold: Option[Double]) extends EvaluationFunction {

  override def toString: String =
    s"In(Set(${setOfElements.take(3).mkString(",")},...[${setOfElements.size}]), ${threshold.toString})"

  override val supportedTypes: Seq[ScannerType] = Seq(StringType)
  private lazy val upperCaseSetOfElements: HashSet[String] = setOfElements.map(_.toUpperCase)

  private val MAX_THRESHOLD: Double = 1.0d
  private val MIN_THRESHOLD: Double = 0.5d
  private val HIGH_THRESHOLD: Double = 0.75d

  private lazy val maxLength = setOfElements.map(_.length).max
  private lazy val maxPossibleDatumStrLength = Math.floor((1 - levThreshold) * maxLength)

  private lazy val levThreshold: Double = threshold.getOrElse(MAX_THRESHOLD) match {
    case x if x < MIN_THRESHOLD => MIN_THRESHOLD
    case x if x > MAX_THRESHOLD => MAX_THRESHOLD
    case x                      => x
  }

  def evaluateDatum(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Confidence = {

    if (threshold.nonEmpty && datum.asInstanceOf[String].length > (maxLength + maxPossibleDatumStrLength)) {
      return NO_MATCH
    }

    val datumStr = datum.asInstanceOf[String].toUpperCase

    // If direct match, check if datumStr length is greater than 2
    if (upperCaseSetOfElements.contains(datumStr)) { return HIGH_CONFIDENCE }
    // If no direct match, calculate Levenshtein and returns appropriate confidence
    calculateLevenshtein(datumStr) match {
      case 0.0d                                  => NO_MATCH
      case levResult if levResult < levThreshold => NO_MATCH
      case levResult =>
        if (levResult >= HIGH_THRESHOLD && datumStr.length >= 2) HIGH_CONFIDENCE // Levenshtein >= 0.75d
        else {
          if (datumStr.length <= 1 || levResult <= MIN_THRESHOLD)
            LOW_CONFIDENCE // Match with 1 letter or Levenshtein <= 0.5
          else MEDIUM_CONFIDENCE // Levenshtein between 0.5 and 0.75
        }
    }
  }

  private def calculateLevenshtein(datumStr: String): Double = {
    var bestResult: Double = 0.0d
    // If levThreshold is 1, we cannot calculate levenshtein
    if (levThreshold < MAX_THRESHOLD) {
      // Extracts the best Levenshtein result
      setOfElements.find {
        elem =>
          // Calculate max num of edit to avoid calculating Levenshtein if not necessary
          val longestString = Math.max(elem.length, datumStr.length)
          val stringDif = Math.abs(elem.length - datumStr.length)
          val maxNumOfEdits = Math
            .floor((1 - (if (bestResult >= levThreshold) HIGH_THRESHOLD else levThreshold)) * longestString)
          if (stringDif <= maxNumOfEdits) {
            Levenshtein.score(datumStr, elem.toUpperCase) match {
              case score if score >= HIGH_THRESHOLD => return score
              case score if score > bestResult      => bestResult = score
              case _                                => ()
            }
          }
          false
      }
    }
    bestResult
  }

}

private[impl] object In extends KeywordDSL {

  override val keyword: String = "in"

  def apply(setOfElements: HashSet[String]): In = In(setOfElements, None)

  def noLevDSL(strings: Set[String]): FunctionDSL = s"$keyword(${strings.map(e => s""""$e"""").mkString(",")})"

  def levDSL(strings: Set[String], threshold: Double): FunctionDSL =
    s"$keyword(${strings.map(e => s""""$e"""").mkString(",")}, $threshold)"

}
