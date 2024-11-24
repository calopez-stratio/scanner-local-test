package org.example.scanner.impl.evaluation

import org.example.scanner.impl.utils.FileUtils
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}

private[impl] object FileContains extends KeywordDSL {

  /**
   * Base DSL function name
   */
  override val keyword: FunctionDSL = "file_contains"

  /**
   * Given a text file with text fragments, applies [[Contains]] function with those text fragments to current datum. It
   * is possible to restrict matches to case-sensitive or ignore case.
   *
   * @param filename
   *   Relative file path (from [[FileUtils.rootPath]]) to the file containing the text fragments.
   * @param caseSensitive
   *   Ignore or restrict match with case.
   *
   * @return
   *   [[Contains]] instance with resulting [[com.stratio.scanner.sdk.ConfidenceEnum.Confidence]]
   */
  def fileContainsFunction(filename: String, caseSensitive: Boolean): Contains = {
    val rawTextFragments: Seq[String] = FileUtils.getFileContainsResource(filename)
    val caseTextFragments: Seq[String] = if (!caseSensitive) rawTextFragments.map(_.toLowerCase) else rawTextFragments
    Contains.apply(caseTextFragments, caseSensitive)
  }

  /**
   * String that defines the two-argument function call variant that this class represents.
   *
   * @param filename
   *   Relative file path (from [[FileUtils.rootPath]]) to the file containing the text fragments.
   * @param caseSensitive
   *   Ignore or restrict match with case.
   * @return
   *   Call to the DSL function in String format
   */
  def fileContainsDSL(filename: String, caseSensitive: Boolean): FunctionDSL =
    s"""$keyword("$filename", $caseSensitive)"""

  /**
   * Given a text file with text fragments, applies [[Contains]] function with those text fragments to current datum.
   * This function ignores upper and/or lower case in the containment check.
   *
   * @param filename
   *   Relative file path (from [[FileUtils.rootPath]]) to the file containing the text fragments.
   * @return
   *   [[Contains]] instance with resulting [[com.stratio.scanner.sdk.ConfidenceEnum.Confidence]]
   */
  def caseSensitiveFileContainsFunction(filename: String): Contains =
    fileContainsFunction(filename, caseSensitive = true)

  /**
   * String that defines the one-argument function call variant that this class represents.
   *
   * @param filename
   *   Relative file path (from [[FileUtils.rootPath]]) to the file containing the text fragments.
   * @return
   *   Call to the DSL function in String format
   */
  def caseSensitiveFileContainsDSL(filename: String): FunctionDSL = s"""$keyword("$filename")"""

}