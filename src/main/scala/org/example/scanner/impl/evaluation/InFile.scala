package org.example.scanner.impl.evaluation

import org.example.scanner.impl.utils.FileUtils
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}


private[impl] object InFile extends KeywordDSL {

  override val keyword: FunctionDSL = "in_file"

  def noLevenshteinFunction(filename: String): In = levenshteinFunction(filename, None)

  def noLevenshteinDSL(filename: String): FunctionDSL = s"""$keyword("$filename")"""

  def levenshteinFunction(filename: String, maybeThreshold: Option[Double]): In =
    In.apply(FileUtils.getInFileResource(filename), maybeThreshold)

  def levenshteinDSL(filename: String, threshold: Double): FunctionDSL = s"""$keyword("$filename", $threshold)"""

}
