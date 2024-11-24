package org.example.scanner.evaluation.config

import org.example.scanner.impl.config.ScannerConfig

object GlobalConfig {
  private[config] val globalConfigKey: String = "global"
  private[config] val globalTrim: String = "trim_datum_before_scanning"

  lazy val trimEnable: Boolean = ScannerConfig.getOrElse(s"$globalConfigKey.$globalTrim", true)
}
