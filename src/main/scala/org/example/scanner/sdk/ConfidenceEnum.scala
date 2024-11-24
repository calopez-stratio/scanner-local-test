package org.example.scanner.sdk

object ConfidenceEnum extends Enumeration {

  type ConfidenceType = Value
  type Confidence = Option[ConfidenceType]
  private[scanner] val VERY_HIGH: ConfidenceType = Value(4, "very_high")
  private[scanner] val HIGH: ConfidenceType = Value(3, "high")
  private[scanner] val MEDIUM: ConfidenceType = Value(2, "medium")
  private[scanner] val LOW: ConfidenceType = Value(1, "low")
  private[scanner] val VERY_LOW: ConfidenceType = Value(0, "very_low")
  private[scanner] val NONE: ConfidenceType = Value(-1, "none")
  private[scanner] val IGNORE: ConfidenceType = Value(-2, "ignore")

  /** To determine HIGH confidence on a datum */
  val HIGH_CONFIDENCE: Confidence = Some(HIGH)

  /** To determine MEDIUM confidence on a datum */
  val MEDIUM_CONFIDENCE: Confidence = Some(MEDIUM)

  /** To determine LOW confidence on a datum */
  val LOW_CONFIDENCE: Confidence = Some(LOW)

  /** To determine NO confidence on a datum */
  val NO_MATCH: Confidence = None

  /** To determine IGNORE confidence on a datum. Used by Model functions only. */
  private[scanner] val IGNORE_CONFIDENCE: Confidence = Some(IGNORE)

  private[scanner] def findValue(s: String): Option[ConfidenceType] = values.find(_.toString == s)

  private[scanner] def findValueById(id: Int): Option[ConfidenceType] = values.find(_.id == id)

  private[scanner] def exists(s: String): Boolean = values.exists(_.toString == s)

  private[scanner] def maxConfidence(confidence1: Confidence, confidence2: Confidence): Confidence =
    if (confidence1 >= confidence2) confidence1 else confidence2
}
