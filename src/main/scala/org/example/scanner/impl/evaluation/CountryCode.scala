package org.example.scanner.impl.evaluation

private[scanner] object CountryCode extends Enumeration {
  type CountryCodeType = Value
  val spain: CountryCodeType = Value("ESP")
  val canada: CountryCodeType = Value("CAN")
  val usa: CountryCodeType = Value("USA")
  val uk: CountryCodeType = Value("GBR")
  val france: CountryCodeType = Value("FRA")
  val chile: CountryCodeType = Value("CHL")
  val ecuador: CountryCodeType = Value("ECU")
  val mexico: CountryCodeType = Value("MEX")

  def findValue(s: String): Option[CountryCodeType] = values.find(_.toString == s)

  def makeString: String = values.mkString(", ")

  def isValidCode(s: String): Boolean = values.exists(_.toString == s)
}
