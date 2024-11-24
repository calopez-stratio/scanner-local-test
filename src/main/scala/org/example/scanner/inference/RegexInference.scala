package org.example.scanner.inference

import org.apache.spark.sql.functions.{col, explode, map_keys, map_values}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, functions}
import org.example.scanner.impl.dslfunctions
import org.example.scanner.inference.DataPatternInference.{MaxIsInSize, MaxIsInString}
import org.example.scanner.pattern.{DataPatternComplexity, QueryWithComplexity}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
object RegexInference {

  private val MaxCharsToAnalyze: Int = 50
  private val MaxCharInt: Int = 128
  private val MinCharSequenceToConsider: Int = 1
  private val IsSimilarGroupRegex: Regex = "^\\[.*\\]$".r
  private val NonMergeableTokens: Seq[Char] = Seq('@')
  private val OverfittingRatio = 0.5
  private val AllowSimilarTokensMerge = true
  private val AllowOptionalSimilarTokensMerge = true

  def inferRegex(columnValues: Seq[String]): String = {
    val charMatrix = Array.ofDim[Long](MaxCharInt, MaxCharsToAnalyze)
    columnValues.foreach(extractCharFrequencies(_, charMatrix))
    generateRegex(charMatrix, columnValues.toSet)
  }

  private def extractCharFrequencies(value: String, regexpMatrix: Array[Array[Long]]): Unit =
    value
      .substring(0, Math.min(MaxCharsToAnalyze, value.length))
      .map(_.toInt)
      .zipWithIndex
      .foreach {
        case (charPos, idx) =>
          if (charPos < MaxCharInt) { regexpMatrix(charPos)(idx) += 1 }
          else { // Strange character, so we count it on the last position
            regexpMatrix(MaxCharInt - 1)(idx) += 1
          }
      }

  /**
   * Generate a regex pattern from a character matrix and a set of distinct datums. If the number of distinct datums is
   * greater than the maximum allowed distinct datums, a regex pattern is generated by analyzing each column of the
   * character matrix and combining the results. Otherwise, the regex pattern is generated by escaping each datum in the
   * set and joining them with "|".
   *
   * @param charMatrix
   *   a two-dimensional array of long values representing characters
   * @param distinctDatums
   *   a set of distinct datums to generate a regex pattern from
   * @return
   *   a string representing the generated regex pattern
   */
  private def generateRegex(charMatrix: Array[Array[Long]], distinctDatums: Set[String]): String =
    // Check if the number of distinct datums exceeds the maximum allowed distinct datums
    if (distinctDatums.size > MaxIsInSize) {
      // Get the sizes of each distinct datum
      val charSizes = distinctDatums.map(_.trim.length)
      // Get the maximum and minimum sizes of the distinct datums
      val maxElementSize = charSizes.max
      val minElementSize = charSizes.min

      // Create a list buffer to store the regex patterns for each column of the character matrix
      val regexBuilder = new ListBuffer[String]

      // Iterate through each character position in the matrix, up to a maximum number of characters to analyze
      (0 until MaxCharsToAnalyze).foreach {
        charPos =>
          // Get an array of characters at the current position in the matrix
          val charArray = charMatrix.map(_(charPos))
          // Get a regex pattern for the valid characters in the array
          val regexToAdd = getValidChars(charArray) match {
            case Some(".")        => Some(".")
            case Some(validChars) =>
              // Reduce the regex pattern for the valid characters
              val resultRegex = reduceRegex(validChars)
              // If the resulting regex pattern has only one character, use it as is
              if (resultRegex.replaceAllLiterally("\\", "").length == 1) Some(resultRegex)
              // Otherwise, wrap the pattern in a character class
              else Some(s"[$resultRegex]")
            case _ => None
          }
          // Add the regex pattern to the list buffer, with optional "?" suffix for each character position
          regexToAdd match {
            case Some(value) =>
              if (charPos >= minElementSize) { regexBuilder.append(s"$value?") }
              else { regexBuilder.append(value) }
            case _ => ()
          }
      }

      // If the maximum size of a distinct datum is greater than the maximum number of characters to analyze, add a ".*" suffix
      if (maxElementSize >= MaxCharsToAnalyze) { regexBuilder.append(".*") }

      // Combine consecutive regex patterns in the list buffer and wrap the resulting pattern with start and end markers
      val combinedTokens = combineConsecutiveTokens(regexBuilder).mkString
      "^(?>" + combinedTokens + ")$"
    } else {
      // Escape each datum in the set and join them with "|" to create a regex pattern
      distinctDatums.map(escapeRegex).mkString("^(", "|", ")$")
    }

  /**
   * Extracts all range groups (e.g. "a-z", "0-9") from a string.
   *
   * @param regex
   *   the string to extract range groups from.
   * @return
   *   a sequence of tuples, each containing the start and end characters of a range group.
   */
  private def getRegexRanges(regex: String): Seq[(Char, Char)] =
    regex
      .zipWithIndex
      .filter {
        case (c, idx) => c == '-' && // Find hyphens in the string
          (regex(idx - 1).isDigit && regex(idx + 1).isDigit || // If it's between two digits
            regex(idx - 1).isLetter && regex(idx + 1).isLetter) // or between two letters
      }
      .map {
        case (_, idx) => (regex(idx - 1), regex(idx + 1)) // Map each hyphen to its surrounding characters
      }

  /**
   * Combines two regular expression strings by merging their range groups (if any) and concatenating them.
   *
   * @param regex1
   *   the first regular expression string to combine
   * @param regex2
   *   the second regular expression string to combine
   * @return
   *   a new regular expression string that combines the two input strings
   */
  def combineRegex(regex1: String, regex2: String): String = {
    // A helper function that combines the range groups from two regular expressions
    def combineRangeGroups(sets1: Seq[(Char, Char)], sets2: Seq[(Char, Char)]): Seq[String] = (sets1 ++ sets2)
      .groupBy(identity)
      .map { case (group, _) => s"${group._1}-${group._2}" }
      .toSeq

    // Get the range groups from the input regular expressions
    val groupRegex1 = getRegexRanges(regex1)
    val groupRegex2 = getRegexRanges(regex2)

    // Combine the range groups from both regular expressions
    val combinedGroups = combineRangeGroups(groupRegex1, groupRegex2)

    // Clean the regular expressions by removing any square brackets and concatenate them
    val cleanRegex1 = regex1.replaceAll("[\\[\\]]", "")
    val cleanRegex2 = regex2.replaceAll("[\\[\\]]", "")
    val combinedRegex = cleanRegex1 + cleanRegex2

    // Remove the range groups from the concatenated regular expression and replace them with a single character class
    val combinedRegexWithoutGroups = combinedGroups
      .foldLeft(combinedRegex) { case (acc, comb) => acc.replaceAllLiterally(comb, "").replaceAll(s"[$comb]", "") }
      .toSet
      .mkString
    val reduceAgain = reduceRegex(unescapeRegex(combinedRegexWithoutGroups))
    s"[$reduceAgain${combinedGroups.mkString}]" // Return the new regular expression string
  }

  /**
   * Determines if two strings are similar by checking if they both contain range groups (as defined by the
   * 'IsSimilarGroupRegex' regex pattern).
   *
   * @param s1
   *   the first string to compare
   * @param s2
   *   the second string to compare
   * @return
   *   true if the two strings are similar, false otherwise
   */
  def similarTokens(s1: String, s2: String): Boolean =
    if (
      AllowSimilarTokensMerge && !s1.exists(NonMergeableTokens.contains) && !s2.exists(NonMergeableTokens.contains) &&
        IsSimilarGroupRegex.findFirstIn(s1).isDefined && IsSimilarGroupRegex.findFirstIn(s2).isDefined
    ) {
      // If both strings contain range groups
      val groups1 = getRegexRanges(s1) // Get the range groups for s1
      val groups2 = getRegexRanges(s2) // Get the range groups for s2
      groups1.nonEmpty && groups2.nonEmpty // Return true if both strings have non-empty range groups
    } else false // Otherwise, the strings are not similar

  def similarOptionalTokens(s1: String, s2: String): Boolean =
    AllowOptionalSimilarTokensMerge && similarTokens(s1, s2.replaceAllLiterally("]?", "]"))

  private[inference] def isMergeableToken(s1: String, s2: String): Boolean =
    AllowOptionalSimilarTokensMerge && !s1.exists(NonMergeableTokens.contains) &&
      !s2.exists(NonMergeableTokens.contains) && getRegexRanges(s1).exists {
      group =>
        val token = s"[${group._1}-${group._2}]"
        val res = s2.replaceAll(token, "")
        res.length < s2.length
    }

  /**
   * Combines consecutive strings in a sequence of strings. If two or more consecutive strings are identical or similar
   * (as determined by the 'similarTokens' function), they are merged together with a count of the number of consecutive
   * strings.
   *
   * @param tokens
   *   the sequence of strings to combine
   * @return
   *   a new sequence of strings with consecutive strings merged
   */
  def combineConsecutiveTokens(tokens: Seq[String]): Seq[String] = {
    def numTokenString(token: String, min: Int, max: Int): String =
      if (max > 1) {
        if (min == max) if (token.endsWith("?")) s"${token.dropRight(1)}{0,$min}" else s"$token{$min}"
        else s"$token{$min,$max}"
      } else token

    val (result, current, countMin, countMax) = tokens.foldLeft((Seq.empty[String], "", 0, 0)) {
      // First iteration
      case ((accumulator, _, 0, 0), token) => (accumulator, token, 1, 1)

      // If repeated token
      case ((accumulator, prevToken, numConsecutiveMin, numConsecutiveMax), token) if prevToken == token =>
        (accumulator, prevToken, numConsecutiveMin + 1, numConsecutiveMax + 1)

      // If similar
      case ((accumulator, prevToken, numConsecutiveMin, numConsecutiveMax), token) if similarTokens(prevToken, token) =>
        (accumulator, combineRegex(prevToken, token), numConsecutiveMin + 1, numConsecutiveMax + 1)

      // If similar with '?'
      case ((accumulator, prevToken, numConsecutiveMin, numConsecutiveMax), token)
        if similarOptionalTokens(prevToken, token) =>
        (
          accumulator,
          combineRegex(prevToken, token.replaceAllLiterally("]?", "]")),
          numConsecutiveMin,
          numConsecutiveMax + 1
        )

      // If mergeable token, ending with '?'
      case ((accumulator, prevToken, numConsecutiveMin, numConsecutiveMax), token)
        if isMergeableToken(prevToken, token) && token.endsWith("?") =>
        (accumulator, combineRegex(prevToken, token.dropRight(1)), numConsecutiveMin, numConsecutiveMax + 1)

      // If mergeable token, without '?'
      case ((accumulator, prevToken, numConsecutiveMin, numConsecutiveMax), token)
        if isMergeableToken(prevToken, token) =>
        (accumulator, combineRegex(prevToken, token), numConsecutiveMin + 1, numConsecutiveMax + 1)

      // If no new consecutive token, end numConsecutive and build the token
      case ((accumulator, prevToken, numConsecutiveMin, numConsecutiveMax), token) =>
        (accumulator :+ numTokenString(prevToken, numConsecutiveMin, numConsecutiveMax), token, 1, 1)
    }
    // Build the last token
    result :+ numTokenString(current, countMin, countMax)
  }

  // Returns all possible characters or a None if strange character is found in this position
  private def getValidChars(charArray: Array[Long]): Option[String] =
    if (charArray(MaxCharInt - 1) > 0) Some(".")
    else Some(charArray.zipWithIndex.filter(_._1 > MinCharSequenceToConsider).map(_._2.toChar).mkString)
      .filter(_.nonEmpty)

  /**
   * Reduces a regular expression based on the input parameters.
   *
   * @param validChars
   *   a string of valid characters
   * @param clean
   *   is the output regex must be cleaned with backslashes
   * @return
   *   a reduced regular expression
   */
  def reduceRegex(validChars: String, clean: Boolean = true): String = {
    var minTextBlock: Int = 25 // Minimum number of consecutive text characters
    var minDigitBlock: Int = 10 // Minimum number of consecutive digit characters
    val minTextBlockRange: Int = 23 // Range for minTextBlock
    val minDigitBlockRange: Int = 8 // Range for minDigitBlock
    val reduceFactor: Double = 1.0 - OverfittingRatio // Calculate reduce factor based on overfitting parameter

    // Reduce the minimum block sizes based on the reduce factor
    if (OverfittingRatio != 1.0) {
      minTextBlock = (minTextBlock.toLong - minTextBlockRange.toDouble * reduceFactor.round).toInt
      minDigitBlock = (minDigitBlock.toLong - minDigitBlockRange.toDouble * reduceFactor.round).toInt
    }

    // Replace consecutive text and digit characters that are above the minimum block sizes with placeholders
    val sortCleanValidChars = escapeRegex(validChars.sorted)
    sortCleanValidChars
      .replaceAll("[A-Z]{" + minTextBlock + ",30}", "A-Z")
      .replaceAll("[a-z]{" + minTextBlock + ",30}", "a-z")
      .replaceAll("[0-9]{" + minDigitBlock + ",10}", "0-9")
  }

  def escapeRegex(s: String): String =
    s.replaceAll("""\\""", """\\\\""")
      .replaceAll("/", """\\/""")
      .replaceAll("""\.""", """\\.""")
      .replaceAll("""\?""", """\\?""")
      .replaceAll("""\[""", """\\[""")
      .replaceAll("]", """\\]""")
      .replaceAll("""\{""", """\\{""")
      .replaceAll("}", """\\}""")
      .replaceAll("""\(""", """\\(""")
      .replaceAll("""\)""", """\\)""")
      .replaceAll("-", """\\-""")
      .replaceAll("""\^""", """\\\^""")
      .replaceAll("""\$""", """\\\$""")
      .replaceAll("""\|""", """\\|""")
      .replaceAll("""\*""", """\\*""")
      .replaceAll("""\+""", """\\+""")

  def unescapeRegex(s: String): String =
    s.replaceAll("""\\\\""", """\\""")
      .replaceAll("""\\/""", "/")
      .replaceAll("""\\\.""", ".")
      .replaceAll("""\\\?""", "?")
      .replaceAll("""\\\[""", "[")
      .replaceAll("""\\]""", "]")
      .replaceAll("""\\\{""", "{")
      .replaceAll("""\\}""", "}")
      .replaceAll("""\\\(""", "(")
      .replaceAll("""\\\)""", ")")
      .replaceAll("""\\-""", "-")
      .replaceAll("""\\\^""", "^")
      .replaceAll("""\\\$""", "\\$")
      .replaceAll("""\\\|""", "|")
      .replaceAll("""\\\*""", "*")
      .replaceAll("""\\\+""", "+")

  private[inference] def getColumnsStringValues(df: DataFrame): Seq[Seq[String]] =
    df.schema
      .fields
      .flatMap {
        field =>
          val columnName = field.name
          field.dataType match {
            case _: StructType => getColumnsStringValues(df.select(columnName + ".*"))
            case _: ArrayType  =>
              // TODO: Cuidado que esto puede petar
              getColumnsStringValues(df.select(explode(col(columnName)).alias("array_col")))
            case _: MapType => getColumnsStringValues(df.select(map_keys(col(columnName)).alias("map_col_key"))) ++
              getColumnsStringValues(df.select(map_values(col(columnName)).alias("map_col_value")))
            case _: StringType => Seq(
              df.select(columnName)
                .na
                .drop
                .filter(col(columnName).isNotNull)
                .where(functions.length(functions.col(columnName)) < MaxIsInString)
                .collect()
                .map(_.getString(0))
                .toSeq
            )
            case _ => None
          }
      }

  def inferRLikeQuery(df: DataFrame): Seq[QueryWithComplexity] = {
    val colsStringValues = getColumnsStringValues(df)
    colsStringValues
      .map {
        values =>
          val regex = RegexInference.inferRegex(values)
          val escaped = regex.replaceAll("""\\""", """\\\\""")
          QueryWithComplexity(dslfunctions.rLike(escaped), DataPatternComplexity.RlikeFunction)
      }
      .distinct
  }

}
