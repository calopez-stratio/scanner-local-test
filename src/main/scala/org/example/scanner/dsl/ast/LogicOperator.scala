package org.example.scanner.dsl.ast

import org.example.scanner.sdk.traits.dsl.DatumFunction

abstract private[scanner] class LogicOperator extends DatumFunction {
  val lhs: DatumFunction
  val rhs: DatumFunction
}
