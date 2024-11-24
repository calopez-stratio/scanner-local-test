package org.example.scanner.dsl.ast

import org.example.scanner.sdk.traits.dsl.{DataPatternExpression, DatumFunction}

private[scanner] case class DatumAndColumnFunction(datumFunction: DatumFunction, columnFunction: ColumnFunction)
  extends DataPatternExpression
