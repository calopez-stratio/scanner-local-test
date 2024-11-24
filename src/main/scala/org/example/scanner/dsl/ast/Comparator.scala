package org.example.scanner.dsl.ast

private[scanner] object Comparator extends Enumeration {

  type ComparatorType = Value
  val eq: Value = Value("=")
  val eqq: Value = Value("==")
  val gte: Value = Value(">=")
  val lte: Value = Value("<=")
  val lt: Value = Value("<")
  val gt: Value = Value(">")

  def findValue(s: String): Option[Value] =
    values.find(_.toString == s) match {
      case Some(Comparator.`eqq`) => Some(Comparator.eq)
      case x                      => x
    }

  def exists(s: String): Boolean = values.exists(_.toString == s)
}

