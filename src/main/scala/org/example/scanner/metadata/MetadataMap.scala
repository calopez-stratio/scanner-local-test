package org.example.scanner.metadata

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.example.scanner.impl.utils.{DataFrameUtils, StructTypeUtils}
import org.example.scanner.sdk.traits.dsl.ColumnMetadata

case class MetadataMap(metadataMap: Map[String, Set[ColumnMetadata]])

object MetadataMap {

  def apply[T <: ColumnMetadata](metadata: T): MetadataMap = new MetadataMap(Map("column" -> Set(metadata)))

  private def reduceMetadataList(metadataSeq: Seq[(String, Set[ColumnMetadata])]): Seq[(String, Set[ColumnMetadata])] =
    metadataSeq
      .groupBy(_._1)
      .map {
        case (columnName: String, columnMetadataMap: Seq[(String, Set[ColumnMetadata])]) =>
          val reduceColumnMetadataMap = columnMetadataMap.map(_._2).reduce(ColumnMetadata.reduce)
          (columnName, reduceColumnMetadataMap)
      }
      .toList

  private def processMapMetadata(
                                  dataMap: Map[Any, Any],
                                  mapType: MapType,
                                  fieldName: String
                                ): Seq[(String, Set[ColumnMetadata])] = {
    val schema = StructType(Seq(StructField("key", mapType.keyType), StructField("value", mapType.valueType)))
    dataMap
      .toSeq
      .flatMap {
        case (key, value) =>
          val row = new GenericRowWithSchema(Array(key, value), schema)
          processRowMetadata(row, Some(fieldName))
      }
  }

  private def processRowMetadata(row: Row, parentName: Option[String] = None): Map[String, Set[ColumnMetadata]] =
    (row.toSeq, row.schema.fields)
      .zipped
      .flatMap {
        case (datum, structField) =>
          val fieldName = StructTypeUtils.getColumnName(parentName, structField.name)
          (datum, structField.dataType) match {
            case (datumStruct: Row, _) => processRowMetadata(datumStruct, Some(fieldName))

            case (datumArray: Seq[Any], arrayType: ArrayType) =>
              val arrayInnerType = DataFrameUtils.flattenArrayType(arrayType)
              val columnMetadataMap = StructTypeUtils
                .flattenArray(datumArray)
                .flatMap {
                  case row: Row => processRowMetadata(row, Some(fieldName))
                  case mapData: Map[Any, Any] =>
                    processMapMetadata(mapData, arrayInnerType.asInstanceOf[MapType], fieldName)
                  case datum => Map(ColumnMetadata.extractMetadata(datum, fieldName))
                }
              reduceMetadataList(columnMetadataMap)

            case (datumMap: Map[Any, Any], mapType: MapType) =>
              val metadataList = processMapMetadata(datumMap, mapType, fieldName)
              reduceMetadataList(metadataList)

            case _ => Map(ColumnMetadata.extractMetadata(datum, fieldName))
          }
      }
      .toMap

  def processMetadata(partition: Iterator[Row]): Iterator[MetadataMap] =
    partition.map(row => MetadataMap(processRowMetadata(row)))

  def reduce: (MetadataMap, MetadataMap) => MetadataMap =
    (m1: MetadataMap, m2: MetadataMap) => {
      val metadataReduce = (m1.metadataMap.toList ++ m2.metadataMap.toList)
        .groupBy(_._1)
        .map {
          case (columName, metadataElements) => columName -> metadataElements.map(_._2).reduce(ColumnMetadata.reduce)
        }
      MetadataMap(metadataReduce)
    }

}
