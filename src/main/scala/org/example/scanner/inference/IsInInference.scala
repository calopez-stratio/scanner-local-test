package org.example.scanner.inference


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.example.scanner.dsl.ast.Comparator
import org.example.scanner.impl.dslfunctions
import org.example.scanner.inference.DataPatternInference.{MaxIsInSize, MaxIsInString}
import org.example.scanner.pattern.{DataPatternComplexity, QueryWithComplexity}

object IsInInference {

  private val FreqItemsPercentages: Seq[Double] = Seq(0.05, 0.1, 0.2, 0.3, 0.4, 0.5)

  private[inference] def inferIsInQuery(df: DataFrame): Seq[QueryWithComplexity] = {
    val colsFreqItems = getColumnsFreqItems(df)
    generateIsInQueries(colsFreqItems)
  }

  private def getColumnsFreqItems(df: DataFrame): Seq[Set[String]] =
    df.schema
      .fields
      .flatMap {
        field =>
          val columnName = field.name
          field.dataType match {
            case _: StructType => getColumnsFreqItems(df.select(columnName + ".*"))
            case _: ArrayType  =>
              // TODO: Cuidado que esto puede petar
              getColumnsFreqItems(df.select(explode(col(columnName)).alias("array_col")))
            case _: MapType => getColumnsFreqItems(df.select(map_keys(col(columnName)).alias("map_col_key"))) ++
              getColumnsFreqItems(df.select(map_values(col(columnName)).alias("map_col_value")))
            case _: StringType => Seq(getFreqItems(columnName, df))
            case _             => None
          }
      }

  private[inference] def getFreqItems(colName: String, df: DataFrame): Set[String] =
    FreqItemsPercentages
      .flatMap {
        freq =>
          df.filter(col(colName).isNotNull)
            .stat
            .freqItems(Array(colName), freq)
            .collect()
            .head
            .getSeq[String](0)
            .filter(d => null != d && d.length <= MaxIsInString)
      }
      .toSet

  private[inference] def generateIsInQueries(colsFreqItems: Seq[Set[String]]): Seq[QueryWithComplexity] =
    colsFreqItems
      .filter(_.nonEmpty)
      .distinct
      .flatMap {
        listOfValues =>
          // This will generate in functions, but you could generate here in with threshold
          listOfValues.size match {
            case size if size > MaxIsInSize => None
            case 1 =>
              val func = dslfunctions
                .buildQuery(dslfunctions.columnKeyword, Comparator.eq.toString, dslfunctions.literal(listOfValues.head))
              Some(QueryWithComplexity(func, DataPatternComplexity.InFunction))
            case _ => Some(QueryWithComplexity(dslfunctions.in(listOfValues), DataPatternComplexity.InFunction))
          }
      }

}
