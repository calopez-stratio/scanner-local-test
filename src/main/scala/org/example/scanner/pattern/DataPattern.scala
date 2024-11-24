package org.example.scanner.pattern

import akka.event.slf4j.SLF4JLogging
import org.example.scanner.dsl.ast.ColumnFunction
import org.example.scanner.evaluation.PatternMatch
import org.example.scanner.sdk.ConfidenceEnum.ConfidenceType
import org.example.scanner.sdk.traits.dsl.{ColumnMetadata, DatumFunction}
import org.example.scanner.sdk.types._

import scala.util.{Failure, Success, Try}

case class DataPattern(
                        id: Int,
                        metadataPath: String,
                        name: String,
                        query: String,
                        attributes: Seq[DataPatternAttribute],
                        complexity: Double = 1.0d
                      ) extends SLF4JLogging {

  private lazy val evaluator: DataPatternExpression = DataPatternExpression(query)
  lazy val maybeDatumFunction: Option[DatumFunction] = evaluator.maybeDatumFunction

  // Ask the datumFunction if any operation inside uses a model to load it beforehand
  lazy val usesModel: Boolean = evaluator.usesModel

  lazy val maybeColumnFunction: Option[ColumnFunction] = evaluator.maybeColumnFunction

  def evaluateColumnFunction(
                              colName: String,
                              maybeConfidences: Option[Map[ConfidenceType, Int]]
                            ): Option[Map[ConfidenceType, Int]] =
    if (maybeColumnFunction.isDefined) {
      maybeColumnFunction.flatMap {
        columnFunction => columnFunction.evaluate(colName, maybeConfidences, evaluator.hasDatumFunction)
      }
    } else { maybeConfidences }

  def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], scannerType: ScannerType): Option[PatternMatch] =
    evaluator.evaluate(datum).map(confidence => PatternMatch(this, Map(confidence -> 1)))
  private def internalTestEvaluator: Try[Option[Map[ConfidenceType, Int]]] =
    Try {
      evaluate("test")(Set.empty, StringType)
      evaluateColumnFunction("test", None)
    }

  /**
   * Method to validate this [[DataPattern]]
   * @return
   *   A Boolean representing if this Data Pattern [[query]] is valid.
   */
  def testEvaluator: Boolean =
    internalTestEvaluator match {
      case Failure(e) =>
        log.warn(s"Invalid query on this ${this.toString}. Error: ${e.getMessage.replaceAll("\n", "\t")}")
        false
      case Success(_) => true
    }
}

object DataPattern {

  def apply(query: String): DataPattern = apply("test", query)

  def apply(name: String, query: String): DataPattern = apply(-1, name, query)

  def apply(id: Int, name: String, query: String): DataPattern = apply(id, "", name, query)

  def apply(id: Int, metadataPath: String, name: String, query: String): DataPattern =
    DataPattern(id, metadataPath, name, query, Seq.empty[DataPatternAttribute])

  def apply(id: Int, name: String, query: String, complexity: Double): DataPattern =
    DataPattern(
      id = id,
      metadataPath = "",
      name,
      query,
      attributes = Seq.empty[DataPatternAttribute],
      complexity = complexity
    )

}
