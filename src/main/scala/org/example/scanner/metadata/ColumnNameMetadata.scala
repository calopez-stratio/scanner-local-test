package org.example.scanner.metadata

import org.example.scanner.sdk.traits.dsl.ColumnMetadata

case class ColumnNameMetadata(columnName: String) extends ColumnMetadata

object ColumnNameMetadata extends ColumnMetadataExtractor {
  override def extractMetadata(datum: Any, colName: String): Option[ColumnMetadata] = Some(ColumnNameMetadata(colName))

  override def reduce(metadataList: List[ColumnMetadata]): ColumnMetadata =
    metadataList.head.asInstanceOf[ColumnNameMetadata]

}
