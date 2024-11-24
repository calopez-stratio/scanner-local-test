package org.example.scanner.impl.column

private[scanner] object ColNameRLikeMode extends Enumeration {

  type ColNameRLikeModeType = Value
  val max: ColNameRLikeModeType = Value("max") // - Maximizes confidence if match
  val sum: ColNameRLikeModeType = Value("sum") // - Adds 1 to confidence if match
  val soft_restrictive: ColNameRLikeModeType = Value("soft_restrictive") // - Adds 1 or subtracts 1 to confidence
  val hard_restrictive: ColNameRLikeModeType = Value("hard_restrictive") // - Maximizes or minimizes the confidence
  val strict: ColNameRLikeModeType = Value("strict") // - Adds 1 to confidence or no match
  val check: ColNameRLikeModeType = Value("check") // - Same confidence or no match

  // Class utils impl
  def findValue(s: String): Option[ColNameRLikeModeType] = values.find(_.toString == s)

  def makeString: String = values.mkString(", ")

  def isValidCode(s: String): Boolean = values.exists(_.toString == s)

}
