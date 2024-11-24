package org.example.scanner.impl.evaluation


import org.example.scanner.dsl.ast.DatumAndColumnFunction
import org.example.scanner.impl.column.ColNameRLikeMode
import org.example.scanner.impl.column.ColNameRLikeMode.ColNameRLikeModeType
import org.example.scanner.impl.config.ScannerConfig
import org.example.scanner.impl.evaluation.traits.dsl.DefaultEnhancedDSL
import org.example.scanner.sdk.traits.impl.FunctionDSL
import org.example.scanner.impl.{functions,dslfunctions}

private[impl] object SocialUrl extends DefaultEnhancedDSL[Between] {

  override val keyword: String = "social_url"

  val enhancedColumnRegexDefault: String = "^(.*social.*url.*)$"

  override lazy val colNameMode: ColNameRLikeModeType = ScannerConfig
    .getColNameModeOrElse(s"$keyword.col_name_mode", ColNameRLikeMode.sum)

  private lazy val socialMediaList = ScannerConfig.getOrElse(
    s"$keyword.social_media_list",
    Seq(
      "facebook",
      "instagram",
      "twitter",
      "linkedin",
      "youtube",
      "twitch",
      "whatsapp",
      "discord",
      "wechat",
      "tiktok",
      "douyin",
      "telegram",
      "snapchat",
      "qq",
      "pinterest",
      "reddit",
      "quora"
    )
  )

  override lazy val enhancedDSL: FunctionDSL = dslfunctions.datumAndColumnFunction(
    dslfunctions.buildAndQuery(dslfunctions.url, dslfunctions.contains(socialMediaList, caseSensitive = false)),
    dslfunctions.colNameRLike(enhancedColumnRegex, colNameMode)
  )

  override lazy val enhancedFunction: DatumAndColumnFunction = functions.datumAndColumnFunction(
    functions.andOperator(functions.url, functions.contains(socialMediaList, caseSensitive = false)),
    functions.colNameRLike(enhancedColumnRegex, colNameMode)
  )

  // Not used
  override def apply(): Between = null
}
