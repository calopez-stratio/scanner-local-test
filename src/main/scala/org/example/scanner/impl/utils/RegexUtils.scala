package org.example.scanner.impl.utils

import scala.util.matching.Regex

object RegexUtils {

  def combineRegex(regexList: List[Regex]): Regex = {
    val innerRegex = regexList.foldLeft("")((finalRegex, regex) => finalRegex + '|' + regex).tail
    ("^(" + innerRegex + ")$").r
  }

  def combineStringRegex(regexList: List[String]): String = {
    val combinedRegex = regexList.foldLeft("") {
      (combinedRegex, regex) =>
        var strippedRegex = regex
        if (regex.startsWith("^")) { strippedRegex = strippedRegex.substring(1) }
        if (regex.endsWith("$")) { strippedRegex = strippedRegex.substring(0, strippedRegex.length - 1) }
        if (combinedRegex.isEmpty) { strippedRegex }
        else { List(combinedRegex, strippedRegex).mkString("|") }
    }
    if (combinedRegex.nonEmpty) { "^" + combinedRegex + "$" }
    else { combinedRegex }
  }

}
