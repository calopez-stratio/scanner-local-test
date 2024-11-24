package org.example.scanner.impl.utils

import org.example.scanner.impl.evaluation.{Comparison, In, RLike}
import org.example.scanner.impl.transformation.Literal
import org.example.scanner.sdk.traits.dsl.DatumFunction

object InferenceUtils {

  def extractIsInValues(maybeDatumFunction: Option[DatumFunction]): Set[String] =
    maybeDatumFunction match {
      case Some(datumFunction: In) => datumFunction.setOfElements
      case _                       => Set.empty[String]
    }

  def extractLiteralValue(maybeDatumFunction: Option[DatumFunction]): Option[String] =
    maybeDatumFunction match {
      case Some(datumFunction: Comparison) => datumFunction.rhs match {
        case literal: Literal => Some(literal.value.toString)
        case _                => None
      }
      case _ => None
    }

  def extractRLikeRegex(maybeDatumFunction: Option[DatumFunction]): Option[String] =
    maybeDatumFunction match {
      case Some(datumFunction: RLike) => Some(datumFunction.regex)
      case _                          => None
    }

  def isColWithStringLiteral(maybeDatumFunction: Option[DatumFunction]): Boolean =
    maybeDatumFunction match {
      case Some(datumFunction: Comparison) => datumFunction.rhs match {
        case literal: Literal => literal.value.isInstanceOf[String]
        case _                => false
      }
      case _ => false
    }

  def containsIsInValues(maybeDatumFunction: Option[DatumFunction], value: String): Boolean =
    maybeDatumFunction match {
      case Some(datumFunction: In) => datumFunction.setOfElements.contains(value.toUpperCase)
      case _                       => false
    }

}
