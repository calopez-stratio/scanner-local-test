package org.example.scanner.impl.evaluation

import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.impl.traits.function.FileFunction

private[impl] case class Video() extends FileFunction {

  override val signaturesHex: Seq[String] = Seq(
    "00000018667479706D703432", // mp4
    "000000206674797069736F6D", // mp4
    "0000001C667479706D703432", // mp4
    "1A45DFA3", // WebM
    "000000186674797033677035", // mov
    "00000020667479707174", // mov
    "00000014667479707174202" // mov
  )

}

private[impl] object Video extends DefaultEnhancedDSL[Video] {

  override val keyword: String = "video"

  override val enhancedColumnRegexDefault: String = "^.*(video|mp4|movie)?.*$"

  override lazy val enhancedColumnRegex: String = ScannerConfig
    .getOrElse(s"$keyword.col_name_regex", enhancedColumnRegexDefault)

}
