package org.example.scanner.impl.evaluation

import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.traits.function.FileFunction

private[impl] case class Audio() extends FileFunction {

  override val signaturesHex: Seq[String] = Seq(
    "494433", // mp3
    "FFFB", // mp3
    "FFF3", // mp3
    "FFF2", // mp3
    "52494646", // wav
    "57415645", // wav
    "664C6143", // flac
    "4F676753", // ogg
    "464F524D", // AIFF
    "3026B2758E66CF11A6D900AA0062CE6C" // wma
  )

}

private[impl] object Audio extends DefaultEnhancedDSL[Audio] {

  override val keyword: String = "audio"

  override val enhancedColumnRegexDefault: String = "^.*(audio|music|mp3)?.*$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

}
