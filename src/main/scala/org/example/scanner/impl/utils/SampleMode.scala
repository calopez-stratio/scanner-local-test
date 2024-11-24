package org.example.scanner.impl.utils

object SampleMode extends Enumeration {

  type SampleModeType = Value
  val SAMPLE = Value("sample")
  val PERCENTAGE = Value("percentage")

  def parseValue(s: String): SampleModeType = values.find(_.toString == s).getOrElse(SAMPLE)

}
