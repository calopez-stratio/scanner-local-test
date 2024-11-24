package org.example.scanner.evaluation

import org.example.scanner.pattern.DataPattern
import org.example.scanner.sdk.ConfidenceEnum.ConfidenceType

case class PatternMatch(dataPattern: DataPattern, confidences: Map[ConfidenceType, Int])
