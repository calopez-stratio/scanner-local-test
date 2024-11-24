package org.example.scanner.impl.transformation

import org.example.scanner.dsl.ast.TransformationFunction
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.example.scanner.sdk.traits.impl.KeywordDSL
import org.example.scanner.sdk.types.ScannerType

import scala.util.Try

private[impl] case class Column() extends TransformationFunction {

  override val outputScannerType: ScannerType = null

  override def isCompatibleForParentTypes(parentTypes: Seq[ScannerType])(implicit datumType: ScannerType): Boolean =
    parentTypes.contains(datumType)

  override def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Any =
    Try {
      datum match {
        case x: Byte                 => BigDecimal(x)
        case x: Short                => BigDecimal(x)
        case x: Int                  => BigDecimal(x)
        case x: Long                 => BigDecimal(x)
        case x: Float                => BigDecimal(x.toDouble)
        case x: Double               => BigDecimal(x)
        case x: java.math.BigInteger => BigDecimal(x)
        case x: java.math.BigDecimal => BigDecimal(x)
        case x: String               => Try(BigDecimal(x)).toOption.getOrElse(x)
        case x                       => x
      }
    }.toOption.orNull

}

private[impl] object Column extends KeywordDSL {

  override val keyword: String = "col"

  def column: Column = Column()
}