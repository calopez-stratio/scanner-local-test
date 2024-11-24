package org.example.scanner.metadata

import org.example.scanner.sdk.traits.dsl.ColumnMetadata

trait ColumnMetadataExtractor {
  def extractMetadata(datum: Any, colName: String): Option[ColumnMetadata]
  def reduce(metadataList: List[ColumnMetadata]): ColumnMetadata
}
