package org.example.scanner.dsl.ast

import org.example.scanner.sdk.ConfidenceEnum.ConfidenceType

private[scanner] trait ColumnFunction extends {

  def evaluate(
                colName: String,
                maybeConfidences: Option[Map[ConfidenceType, Int]],
                hasColumnFunction: Boolean = true
              ): Option[Map[ConfidenceType, Int]]

}
