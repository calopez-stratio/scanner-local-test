package org.example.scanner.impl.evaluation

import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.FileExtension.FileExtensionType
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.traits.function.FileFunction
import org.example.scanner.sdk.traits.impl.FunctionDSL

private[impl] case class Document(someFileExtensions: Option[Seq[FileExtensionType]]) extends FileFunction {

  private val fileExtensionToSignatureMap: Seq[String] = Seq(
    "255044462D", // pdf, fdf, ai
    "504B0304", // odt, odp, ott
    "504B0304", // docx, pptx, xlsx
    "D0CF11E0A1B11AE1" // doc, dot, pps, ppt, xla, xls, wiz
  )

  override val signaturesHex: Seq[String] = someFileExtensions
    .map(fileExtensions => fileExtensions.map(fileExt => fileExtensionToSignatureMap(fileExt.id)))
    .getOrElse(fileExtensionToSignatureMap)

}

private[impl] object Document extends DefaultEnhancedDSL[Document] {

  override val keyword: String = "document"

  override val enhancedColumnRegexDefault: String = "^.*(pdf|document|file|archivo|fichero)?.*$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

  def apply(fileExtensions: Seq[FileExtensionType]): Document = apply(Some(fileExtensions))

  def apply(): Document = apply(None)

  def fileExtensionsDSL(someFileExtensions: Seq[FileExtensionType]): FunctionDSL =
    s"""$keyword(${someFileExtensions.map(fileExt => s""""$fileExt"""").mkString(",")})"""

  def fileExtensionsFunction(fileExtensions: Seq[FileExtensionType]): Document = apply(fileExtensions)

}
