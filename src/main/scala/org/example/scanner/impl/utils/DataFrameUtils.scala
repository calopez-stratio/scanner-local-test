package org.example.scanner.impl.utils


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.example.scanner.impl.utils.StructTypeUtils.getColumnName

import scala.annotation.tailrec

object DataFrameUtils {

  @tailrec
  def flattenArrayType(arrayType: ArrayType): DataType =
    arrayType.elementType match {
      case subArrayType: ArrayType => flattenArrayType(subArrayType)
      case otherType               => otherType
    }

  private def getColumnNames[T <: DataType](dataType: T, parentName: Option[String] = None): Seq[String] =
    dataType match {
      case structType: StructType =>
        structType.fields.flatMap(field => getColumnNames(field.dataType, Some(getColumnName(parentName, field.name))))
      case mapType: MapType => getColumnNames(mapType.keyType, Some(getColumnName(parentName, "key"))) ++
        getColumnNames(mapType.valueType, Some(getColumnName(parentName, "value")))
      case arrayType: ArrayType => getColumnNames(flattenArrayType(arrayType), parentName)
      case _                    => Seq(parentName.getOrElse(""))
    }

  implicit class DataFrameImprovements(val df: DataFrame) {
    def allColumnNames: Seq[String] = getColumnNames(df.schema)

    def getRandomSample(nElements: Int): DataFrame = {
      val totalRows = df.count()
      val fraction = math.min(nElements.toDouble, totalRows) / totalRows.toDouble
      df.sample(fraction)
    }

  }

}
