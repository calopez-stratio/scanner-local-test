package org.example.scanner.impl.transformation

import org.example.scanner.dsl.ast.ArithmeticOperator.ArithmeticOperatorType
import org.example.scanner.dsl.ast.{ArithmeticOperator, TransformationFunction}
import org.example.scanner.sdk.traits.dsl.ColumnMetadata
import org.example.scanner.sdk.types.{DoubleType, ScannerType, ScannerTypes}

private[impl] case class ArithmeticFunction(
                                             lhs: TransformationFunction,
                                             operator: ArithmeticOperatorType,
                                             rhs: TransformationFunction
                                           ) extends TransformationFunction {

  override val supportedTypes: Seq[ScannerType] = ScannerTypes.NUMERIC_TYPES

  override val outputScannerType: ScannerType = DoubleType

  override def isCompatibleForParentTypes(parentTypes: Seq[ScannerType])(implicit datumType: ScannerType): Boolean =
    parentTypes.contains(datumType) && lhs.isCompatibleForParentTypes(supportedTypes) &&
      rhs.isCompatibleForParentTypes(supportedTypes)

  private def operate(l: BigDecimal, r: BigDecimal): BigDecimal =
    operator match {
      case ArithmeticOperator.sum => l + r
      case ArithmeticOperator.sub => l - r
      case ArithmeticOperator.mul => l * r
      case ArithmeticOperator.div => l / r
      case ArithmeticOperator.mod => l % r
    }

  def evaluate(datum: Any)(implicit metadataSet: Set[ColumnMetadata], datumType: ScannerType): Any =
    (lhs.evaluate(datum: Any), rhs.evaluate(datum: Any)) match {
      case (seqAges: Seq[_], value: BigDecimal) => seqAges.asInstanceOf[Seq[BigDecimal]].map(age => operate(age, value))
      case (value: BigDecimal, seqAges: Seq[_]) => seqAges.asInstanceOf[Seq[BigDecimal]].map(age => operate(value, age))
      case (l: BigDecimal, r: BigDecimal)       => operate(l, r)
      case (_, _)                               => Seq.empty[BigDecimal]
    }

}
