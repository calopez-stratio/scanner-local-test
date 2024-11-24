package org.example.scanner.dsl.ast

private[scanner] object ArithmeticOperator extends Enumeration {

  type ArithmeticOperatorType = Value
  val sum: Value = Value("+")
  val sub: Value = Value("-")
  val mul: Value = Value("*")
  val div: Value = Value("/")
  val mod: Value = Value("%")

  def findValue(s: String): Option[Value] = values.find(_.toString == s)

  def exists(s: String): Boolean = values.exists(_.toString == s)
}
