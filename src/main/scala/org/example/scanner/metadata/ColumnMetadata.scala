package org.example.scanner.metadata

import org.example.scanner.evaluation.config.GlobalConfig
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import scala.reflect.ClassTag

import scala.collection.mutable

object ColumnMetadata {

  private val extractors: mutable.Set[ColumnMetadataExtractor] = mutable.Set(DateFormatMetadata, ColumnNameMetadata)

  // Reduce two Sets of column metadata
  def reduce: (Set[ColumnMetadata], Set[ColumnMetadata]) => Set[ColumnMetadata] =
    (m1, m2) =>
      (m1.toList ++ m2.toList)
        .groupBy(_.getClass.getSimpleName)
        .map {
          case (_, elementsList) => elementsList match {
            case elements if elements.nonEmpty && elements.head.isInstanceOf[DateFormatMetadata] =>
              DateFormatMetadata.reduce(elements)
            case elements if elements.nonEmpty && elements.head.isInstanceOf[ColumnNameMetadata] =>
              ColumnNameMetadata.reduce(elements)
          }
        }
        .toSet

  // Extracts from given datum any possible metadata
  def extractMetadata(datum: Any, fieldName: String): (String, Set[ColumnMetadata]) =
    fieldName ->
      extractors
        .flatMap(
          e =>
            e.extractMetadata(
              datum match {
                case x: String if GlobalConfig.trimEnable => x.trim
                case x                                    => x
              },
              fieldName
            )
        )
        .toSet

  def extractMetadataForTest(datum: Any): Set[ColumnMetadata] = extractMetadata(datum, "column")._2

  def extractMetadataForAPI(datum: Any, columnName: String): Set[ColumnMetadata] = extractMetadata(datum, columnName)._2

  // Find the given T of metadata in the implicit metadataSet
  def findMetadata[T <: ColumnMetadata](implicit metadataSet: Set[ColumnMetadata], tag: ClassTag[T]): Option[T] =
    metadataSet.find(tag.runtimeClass.isInstance).map(_.asInstanceOf[T])

}
