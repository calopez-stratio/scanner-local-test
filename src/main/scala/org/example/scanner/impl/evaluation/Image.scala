package org.example.scanner.impl.evaluation

import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.traits.function.FileFunction

private[impl] case class Image() extends FileFunction {

  override val signaturesHex: Seq[String] = Seq(
    "504943540008", // img
    "474946383961", // gif
    "89504E470D0A1A0A", // png
    "FFD8" // jpg
  )

}

private[impl] object Image extends DefaultEnhancedDSL[Image] {

  override val keyword: String = "image"

  override val enhancedColumnRegexDefault: String = "^.*(image|png|img|picture|graphic|imagen|foto)?.*$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

}
