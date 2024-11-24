package org.example.scanner.impl.utils

object StructTypeUtils {

  private val StructParentSeparator: String = "."

  /**
   * This function takes a sequence of any type and returns a flattened sequence with all nested arrays unwrapped.
   *
   * @param array
   *   the input sequence to flatten.
   * @return
   *   a flattened sequence with all nested arrays unwrapped.
   */
  def flattenArray(array: Seq[Any]): Seq[Any] =
    array.flatMap {
      case nestedArray: Seq[Any] => flattenArray(nestedArray)
      case element               => Seq(element)
    }

  def getColumnName(parentName: Option[String], fieldName: String): String =
    parentName.map(name => s"$name$StructParentSeparator$fieldName").getOrElse(fieldName)

  /**
   * Create a field path by joining a sequence of field names with a separator.
   *
   * @param fieldNames
   *   a variable number of field names to join into a single path.
   * @return
   *   a string representing the field path.
   */
  def createFieldPath(fieldNames: String*): String = fieldNames.mkString(StructParentSeparator)

}
