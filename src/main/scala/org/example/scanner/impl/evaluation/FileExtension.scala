package org.example.scanner.impl.evaluation

private[scanner] object FileExtension extends Enumeration {
  type FileExtensionType = Value
  val pdf: FileExtensionType = Value(0, "PDF")
  val odt: FileExtensionType = Value(1, "ODT")
  val doc: FileExtensionType = Value(2, "DOC")
  val docx: FileExtensionType = Value(3, "DOCX")

  val fileExtensionToSignatureMap = Map(
    FileExtension.pdf -> "25504446", // pdf, fdf, ai
    FileExtension.odt -> "504B0304", // odt, odp, ott
    FileExtension.docx -> "504B030414000600", // docx, pptx, xlsx
    FileExtension.doc -> "D0CF11E0A1B11AE1" // doc, dot, pps, ppt, xla, xls, wiz
  )

  def findValue(s: String): Option[FileExtensionType] = values.find(_.toString == s)

  def makeString: String = values.mkString(", ")

  def isValidFileExtension(s: String): Boolean = values.exists(_.toString == s)
}