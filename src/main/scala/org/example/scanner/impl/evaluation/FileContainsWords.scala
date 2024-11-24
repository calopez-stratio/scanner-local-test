package org.example.scanner.impl.evaluation

import org.example.scanner.impl.utils.FileUtils
import org.example.scanner.sdk.traits.impl.{FunctionDSL, KeywordDSL}

private[impl] object FileContainsWords extends KeywordDSL {

  /**
   * Base DSL function name
   */
  override val keyword: FunctionDSL = "file_contains_words"

  /**
   * Given a text file with text fragments, applies [[ContainsWords]] function with those text fragments to current
   * datum. It is possible to restrict matches to case-sensitive or ignore case.
   *
   * @param filename
   *   Relative file path (from [[FileUtils.rootPath]]) to the file containing the text fragments.
   * @param caseSensitive
   *   Ignore or restrict match with case.
   *
   * @return
   *   [[ContainsWords]] instance with resulting [[org.example.scanner.sdk.ConfidenceEnum.Confidence]]
   */
  def fileContainsWordsFunction(filename: String, caseSensitive: Boolean): ContainsWords = {
    val rawTextFragments: Seq[String] = FileUtils.getFileContainsResource(filename)
    ContainsWords.apply(rawTextFragments, caseSensitive)
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
  def fileContainsWordsDSL(filename: String, caseSensitive: Boolean): FunctionDSL =
    s"""$keyword("$filename", $caseSensitive)"""

  /**
   * Given a text file with text fragments, applies [[ContainsWords]] function with those text fragments to current
   * datum. This function ignores upper and/or lower case in the containment check.
   *
   * @param filename
   *   Relative file path (from [[FileUtils.rootPath]]) to the file containing the text fragments.
   * @return
   *   [[ContainsWords]] instance with resulting [[org.example.scanner.sdk.ConfidenceEnum.Confidence]]
   */
  def caseSensitiveFileContainsWordsFunction(filename: String): ContainsWords =
    fileContainsWordsFunction(filename, caseSensitive = true)

  /**
   * String that defines the one-argument function call variant that this class represents.
   *
   * @param filename
   *   Relative file path (from [[FileUtils.rootPath]]) to the file containing the text fragments.
   * @return
   *   Call to the DSL function in String format
   */
  def caseSensitiveFileContainsWordsDSL(filename: String): FunctionDSL = s"""$keyword("$filename")"""

}
