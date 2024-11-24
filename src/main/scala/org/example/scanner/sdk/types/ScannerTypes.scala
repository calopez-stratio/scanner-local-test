package org.example.scanner.sdk.types

import org.apache.spark.sql.types.DataType

import scala.annotation.tailrec
import scala.util._

object ScannerTypes {

  @tailrec
  def getScannerType(dataType: DataType): ScannerType =
    dataType match {
      case org.apache.spark.sql.types.ByteType => ByteType
      case org.apache.spark.sql.types.ShortType => ShortType
      case org.apache.spark.sql.types.IntegerType => IntegerType
      case org.apache.spark.sql.types.LongType => LongType
      case org.apache.spark.sql.types.FloatType => FloatType
      case org.apache.spark.sql.types.DoubleType => DoubleType
      case org.apache.spark.sql.types.StringType => StringType
      case org.apache.spark.sql.types.BinaryType => BinaryType
      case org.apache.spark.sql.types.BooleanType => BooleanType
      case org.apache.spark.sql.types.TimestampType => TimestampType
      case org.apache.spark.sql.types.DateType => DateType
      case org.apache.spark.sql.types.ArrayType(elementType, _) => getScannerType(elementType)
      case _: org.apache.spark.sql.types.DecimalType => DecimalType
      case _ => null
    }

  def getScannerTypeFromAny(value: Any): ScannerType =
    value match {
      case _: Byte               => ByteType
      case _: Short              => ShortType
      case _: Int                => IntegerType
      case _: Long               => LongType
      case _: Float              => FloatType
      case _: Double             => DoubleType
      case _: BigInt             => DecimalType
      case _: BigDecimal         => DecimalType
      case _: String             => StringType
      case _: Array[Byte]        => BinaryType
      case _: Boolean            => BooleanType
      case _: java.sql.Timestamp => TimestampType
      case _: java.sql.Date      => DateType
      case _                     => null
    }

  val FUNCTION_DATA_TYPES: Seq[ScannerType] = Seq(
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    NumericType,
    StringType,
    BooleanType,
    TimestampType,
    DateType,
    BinaryType
  )

  def getScannerTypeFromName(name: String): ScannerType =
    FUNCTION_DATA_TYPES.find(scannerType => scannerType.name.equals(name)).orNull

  def inferScannerTypeFromString(value: String): ScannerType = {
    val anyValue = Try {
      val numeric: Any =
        if (value.contains(".") || value.contains(",")) value.replace(",", ".").toDouble else value.toLong
      numeric
    } match {
      case Success(value) => value
      case Failure(_) =>
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) { value.toBoolean }
        else { value }
    }
    getScannerTypeFromAny(anyValue)
  }

  val INTEGER_TYPES: Seq[ScannerType] = Seq(IntegerType, LongType, DecimalType)

  val INTEGER_STRING_TYPES: Seq[ScannerType] = ScannerTypes.INTEGER_TYPES :+ StringType

  val NUMERIC_TYPES: Seq[ScannerType] = Seq(ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)

  val NUMERIC_STRING_TYPES: Seq[ScannerType] = ScannerTypes.NUMERIC_TYPES :+ StringType
}

sealed trait ScannerType {
  val name: String
}

case object ByteType extends ScannerType {
  override val name: String = "byte"
}

case object ShortType extends ScannerType {
  override val name: String = "short"
}

case object IntegerType extends ScannerType {
  override val name: String = "integer"
}

case object LongType extends ScannerType {
  override val name: String = "long"
}

case object FloatType extends ScannerType {
  override val name: String = "float"
}

case object DoubleType extends ScannerType {
  override val name: String = "double"
}

case object StringType extends ScannerType {
  override val name: String = "string"
}

case object BinaryType extends ScannerType {
  override val name: String = "binary"
}

case object BooleanType extends ScannerType {
  override val name: String = "boolean"
}

case object TimestampType extends ScannerType {
  override val name: String = "timestamp"
}

case object DateType extends ScannerType {
  override val name: String = "date"
}

case object DecimalType extends ScannerType {
  override val name: String = "decimal"
}

case object NumericType extends ScannerType {
  override val name: String = "numeric"
}
