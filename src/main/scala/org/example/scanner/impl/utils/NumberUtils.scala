package org.example.scanner.impl.utils

object NumberUtils {

  implicit class BigDecimalImprovements(val d: BigDecimal) {
    def roundToDouble(scale: Int): Double = d.setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

}
