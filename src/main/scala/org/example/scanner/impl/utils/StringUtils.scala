package org.example.scanner.impl.utils

object StringUtils {

  private val specialCharacters = List('!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '+', '=', '{', '}', '[',
    ']', '|', '\\', ':', ';', '<', '>', '?', '~', '`')

  implicit class StringImprovements(val s: String) {
    def removeChar(charToRemove: Char): String = s.filterNot(_.equals(charToRemove))

    def removeChar(charsToRemove: Char*): String =
      charsToRemove.foldLeft(s)((str, char) => str.filterNot(_.equals(char)))

    def containsNumbers: Boolean = s.exists(_.isDigit)

    def containsNumbersOrEspecialChars: Boolean = s.exists(c => c.isDigit || specialCharacters.contains(c))

    def adjustLength(expectedLength: Int, padChar: Char): String = {
      val inputLength = s.length
      if (inputLength == expectedLength) { s }
      else if (inputLength < expectedLength) { padChar.toString * (expectedLength - inputLength) + s }
      else { s.drop(inputLength - expectedLength) }
    }

  }

  def removeTableCollectionName(collectionName: String, tableName: String, columns: Seq[String]): Seq[String] =
    columns.map(_.replaceFirst(s"${collectionName}\\.${tableName}\\.", ""))

  def addTableCollectionName(tableName: String, columns: Seq[String]): Seq[String] =
    columns.map(col => s"$tableName.$col")

}
