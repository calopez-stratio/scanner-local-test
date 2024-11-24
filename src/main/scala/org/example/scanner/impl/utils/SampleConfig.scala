package org.example.scanner.impl.utils

import SampleMode.SampleModeType

case class SampleConfig(
                         sampleMode: SampleModeType = SampleMode.SAMPLE,
                         sampleSize: Int = 10000,
                         sampleForceLimit: Boolean = false,
                         samplePercentage: Double = 10.0d,
                         samplePercentageLimits: Boolean = true,
                         samplePercentageAtLeast: Integer = 1000,
                         samplePercentageAtMost: Integer = 100000,
                         removeNulls: Boolean = true,
                         removeNullsRegex: String = SampleUtils.DEFAULT_REMOVE_REGEX,
                         numCores: Int = 2,
                         partitionMultiplier: Int = 4
                       )
