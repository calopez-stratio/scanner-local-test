package org.example.scanner.dsl.ast

import org.example.scanner.impl.evaluation.{CountryCode, FileExtension}
import org.example.scanner.sdk.types.{NumericType, ScannerTypes}

private[scanner] object ErrorHandling {

  val InvalidIsInThresholdError: String =
    "INVALID ARGUMENT. Threshold out of range. It should be a number within the range (0, 1]."

  val InvalidDataType: String =
    s"INVALID ARGUMENT. DataType does not exist. DataType should be one of the following options:\n${ScannerTypes
      .FUNCTION_DATA_TYPES
      .map(
        scannerType =>
          if (scannerType == NumericType) s"\t- ${scannerType.name} (${ScannerTypes.NUMERIC_TYPES.map(_.name).mkString(", ")})"
          else s"\t- ${scannerType.name}"
      )
      .mkString("\n")}"

  val InvalidFunctionError: String = "INVALID FUNCTION. This function does not exist."

  val CountryFunctionArgumentError: String = "is not a supported country for this function." +
    s"\nPlease, choose one of the following valid countries:\n\t${CountryCode.makeString}."

  val FileExtensionArgumentError: String = "is not a supported file extension for this function." +
    s"\nPlease, choose one of the following valid file extensions:\n\t${FileExtension.makeString}."

  val ColNameRLikeModeArgumentError: String = "is not a supported mode for this function." +
    s"\nPlease, choose one of the following modes:\n\t${CountryCode.makeString}."

  def invalidArgumentError(maybeInput: Option[Any], error: String): String =
    s"""INVALID ARGUMENT. ${maybeInput.getOrElse("")} $error"""

}
