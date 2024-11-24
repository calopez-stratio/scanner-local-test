package org.example.scanner.impl.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

case class RowData(datum: String, columnName: String)
object RowUtils {

  private def getMapData(mapData: Map[Any, Any], parentName: Option[String], mapType: MapType): Seq[RowData] = {
    val schema = StructType(Seq(StructField("key", mapType.keyType), StructField("value", mapType.valueType)))
    mapData
      .toSeq
      .flatMap {
        case (key, value) =>
          val row = new GenericRowWithSchema(Array(key, value), schema)
          getRowData(row, parentName)
      }
  }

  private def getRowData(row: Row, parentName: Option[String] = None): Seq[RowData] =
    row
      .toSeq
      .zip(row.schema.fields)
      .filter {
        case (_: String, _) | (_: Seq[_], _) | (_: Map[_, _], _) | (_: Row, _) => true
        case _                                                                 => false
      }
      .flatMap {
        case (row: Row, field) => getRowData(row, Some(StructTypeUtils.getColumnName(parentName, field.name)))
        case (arr: Seq[Any], field) =>
          val arrayInnerType = DataFrameUtils.flattenArrayType(field.dataType.asInstanceOf[ArrayType])
          StructTypeUtils
            .flattenArray(arr)
            .flatMap {
              case structData: Row =>
                getRowData(structData, Some(StructTypeUtils.getColumnName(parentName, field.name)))
              case mapData: Map[Any, Any] => getMapData(
                mapData,
                Some(StructTypeUtils.getColumnName(parentName, field.name)),
                arrayInnerType.asInstanceOf[MapType]
              )
              case str: String => Some(RowData(str, StructTypeUtils.getColumnName(parentName, field.name)))
            }
        case (mapData: Map[Any, Any], field) => getMapData(
          mapData,
          Some(StructTypeUtils.getColumnName(parentName, field.name)),
          field.dataType.asInstanceOf[MapType]
        )
        case (str: String, field) => Some(RowData(str, StructTypeUtils.getColumnName(parentName, field.name)))
        case (_, _)               => None
      }

  def getRowListData(rowList: List[Row]): List[RowData] = rowList.flatMap(row => getRowData(row))
}
