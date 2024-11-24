package org.example.scanner.evaluation

case class ColumnPatternMatch(colName: String, patternMatches: Set[PatternMatch], columnMetrics: Map[String, Int]) {

  override def toString: String =
    f"ColumnPatternMatch($colName,[${patternMatches.mkString(",")}],[${columnMetrics.mkString(",")}])"

  override def equals(o: Any): Boolean =
    if (!o.isInstanceOf[ColumnPatternMatch]) false
    else {
      val other: ColumnPatternMatch = o.asInstanceOf[ColumnPatternMatch]
      colName == other.colName && patternMatches == other.patternMatches
    }

}

object ColumnPatternMatch {

  def empty(colName: String): ColumnPatternMatch =
    new ColumnPatternMatch(colName, Set.empty[PatternMatch], Map.empty[String, Int])

  def apply(colName: String, results: Iterable[PatternMatch]): ColumnPatternMatch =
    new ColumnPatternMatch(colName, results.toSet, Map.empty[String, Int])

  val METRIC_COUNT_NAME = "NUM_ELEMENTS"
  val METRIC_NULLS_NAME = "NUM_NULL_ELEMENTS"


  def createCountMetrics(isValidDatum: Boolean): Map[String, Int] =
    if (isValidDatum) Map(METRIC_NULLS_NAME -> 0, METRIC_COUNT_NAME -> 1)
    else Map(METRIC_NULLS_NAME -> 1, METRIC_COUNT_NAME -> 0)

  def createMetrics(isValidData: Seq[Boolean]): Map[String, Int] = {
    val nValidData = isValidData.count(_ == true)
    Map(METRIC_NULLS_NAME -> (isValidData.length - nValidData), METRIC_COUNT_NAME -> nValidData)
  }

  def combineMetrics(metrics: Seq[Map[String, Int]]): Map[String, Int] =
    Map(
      METRIC_NULLS_NAME -> metrics.map(_.getOrElse(METRIC_NULLS_NAME, 0)).sum,
      METRIC_COUNT_NAME -> metrics.map(_.getOrElse(METRIC_COUNT_NAME, 0)).sum
    )

}
