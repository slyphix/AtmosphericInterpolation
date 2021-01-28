package netcdf

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule


private[netcdf] trait GenerateNeighborFilter {
  def lt(bound: Any): Expression
  def gt(bound: Any): Expression
}

private[netcdf] class MonotonicGNF(fn: FindNeighbors, substExpr: Expression) extends GenerateNeighborFilter {

  private val search = fn.search.asInstanceOf[NeighborSearch with MonotonicSequence]

  def lt(bound: Any): Expression = {
    val castBound = fn.inputConverter(bound).asInstanceOf[search.In]
    if (search.hasNeighbors(castBound)) {
      val outBound = search.nextLargerNeighbor(castBound)
      val outConverted = fn.outputConverter(outBound)
      expressions.LessThanOrEqual(substExpr, Literal(outConverted, fn.outputType))
    } else {
      expressions.Literal.TrueLiteral
    }
  }
  def gt(bound: Any): Expression = {
    val castBound = fn.inputConverter(bound).asInstanceOf[search.In]
    if (search.hasNeighbors(castBound)) {
      val outBound = search.nextSmallerNeighbor(castBound)
      val outConverted = fn.outputConverter(outBound)
      expressions.GreaterThanOrEqual(substExpr, Literal(outConverted, fn.outputType))
    } else {
      expressions.Literal.TrueLiteral
    }
  }

  def orderReversing: GenerateNeighborFilter = {
    val self = this
    new GenerateNeighborFilter {
      override def lt(bound: Any): Expression = self.gt(bound)
      override def gt(bound: Any): Expression = self.lt(bound)
    }
  }
}

private[netcdf] class WrappingMonotonicGNF(fn: FindNeighbors, substExpr: Expression) extends GenerateNeighborFilter {

  private val search = fn.search.asInstanceOf[NeighborSearch with WrappingMonotonicSequence]

  private val smallestNeighborConverted = fn.outputConverter(search.smallestPossibleNeighbor)
  private val largestNeighborConverted = fn.outputConverter(search.largestPossibleNeighbor)

  def lt(bound: Any): Expression = {
    val castBound = fn.inputConverter(bound).asInstanceOf[search.In]
    if (search.isMaxSideOverhang(castBound))
      return expressions.Literal.TrueLiteral

    val outBound = search.nextLargerNeighbor(castBound)
    val outConverted = fn.outputConverter(outBound)
    val newCondition = expressions.LessThanOrEqual(substExpr, Literal(outConverted, fn.outputType))

    // second condition prevents filters like (n <= 5) || (n == 5)
    if (search.hasMinSideOverhang && !largestNeighborConverted.equals(outConverted)) {
      expressions.Or(
        newCondition,
        expressions.EqualTo(substExpr, Literal(largestNeighborConverted, fn.outputType)))
    } else {
      newCondition
    }
  }
  def gt(bound: Any): Expression = {
    val castBound = fn.inputConverter(bound).asInstanceOf[search.In]
    if (search.isMinSideOverhang(castBound))
      return expressions.Literal.TrueLiteral

    val outBound = search.nextSmallerNeighbor(castBound)
    val outConverted = fn.outputConverter(outBound)
    val newCondition = expressions.GreaterThanOrEqual(substExpr, Literal(outConverted, fn.outputType))

    if (search.hasMaxSideOverhang && !smallestNeighborConverted.equals(outConverted)) {
      expressions.Or(
        newCondition,
        expressions.EqualTo(substExpr, Literal(smallestNeighborConverted, fn.outputType)))
    } else {
      newCondition
    }
  }
}

private[netcdf] class ReverseWrappingMonotonicGNF(fn: FindNeighbors, substExpr: Expression) extends GenerateNeighborFilter {

  private val search = fn.search.asInstanceOf[NeighborSearch with WrappingMonotonicSequence]

  private val smallestNeighborConverted = fn.outputConverter(search.smallestPossibleNeighbor)
  private val largestNeighborConverted = fn.outputConverter(search.largestPossibleNeighbor)

  def lt(bound: Any): Expression = {
    val castBound = fn.inputConverter(bound).asInstanceOf[search.In]
    if (search.isMinSideOverhang(castBound))
      return expressions.Literal.TrueLiteral

    val outBound = search.nextSmallerNeighbor(castBound)
    val outConverted = fn.outputConverter(outBound)
    val newCondition = expressions.GreaterThanOrEqual(substExpr, Literal(outConverted, fn.outputType))

    if (search.hasMinSideOverhang && !smallestNeighborConverted.equals(outConverted)) {
      expressions.Or(
        newCondition,
        expressions.EqualTo(substExpr, Literal(smallestNeighborConverted, fn.outputType)))
    } else {
      newCondition
    }
  }
  def gt(bound: Any): Expression = {
    val castBound = fn.inputConverter(bound).asInstanceOf[search.In]
    if (search.isMaxSideOverhang(castBound))
      return expressions.Literal.TrueLiteral

    val outBound = search.nextLargerNeighbor(castBound)
    val outConverted = fn.outputConverter(outBound)
    val newCondition = expressions.LessThanOrEqual(substExpr, Literal(outConverted, fn.outputType))

    if (search.hasMaxSideOverhang && !largestNeighborConverted.equals(outConverted)) {
      expressions.Or(
        newCondition,
        expressions.EqualTo(substExpr, Literal(largestNeighborConverted, fn.outputType)))
    } else {
      newCondition
    }
  }
}


object PushNeighborFilter extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case g @ Generate(fn @ FindNeighbors(search, inputExpression), _, _, _, Seq(outputAttr), child)
        if search.isInstanceOf[MonotonicSequence] || search.isInstanceOf[WrappingMonotonicSequence] =>

      val constraints = child.constraints.asInstanceOf[Set[Expression]]

      val gen: GenerateNeighborFilter = search match {
        case s: MonotonicSequence =>
          val gnf = new MonotonicGNF(fn, outputAttr)
          if (s.isOrderReversing) gnf.orderReversing else gnf
        case s: WrappingMonotonicSequence =>
          if (s.isOrderReversing) new ReverseWrappingMonotonicGNF(fn, outputAttr) else new WrappingMonotonicGNF(fn, outputAttr)
      }

      val transformedConstraints = constraints.flatMap { filter =>
        val transformed = transformFiltersForMatchingExpression(filter, inputExpression, gen)
        if (transformed == filter) None else Some(transformed)
      }

      if (transformedConstraints.isEmpty) g else Filter(transformedConstraints.reduce(expressions.And), g)
  }

  private def transformFiltersForMatchingExpression(
    rootFilter: Expression,
    expressionToMatch: Expression,
    gen: GenerateNeighborFilter
  ): Expression = {

    rootFilter transformUp {
      case expressions.GreaterThanOrEqual(ex, lit: Literal) if ex.semanticEquals(expressionToMatch) =>
        gen.gt(lit.value)

      case expressions.GreaterThan(ex, lit: Literal) if ex.semanticEquals(expressionToMatch) =>
        gen.gt(lit.value)

      case expressions.LessThanOrEqual(ex, lit: Literal) if ex.semanticEquals(expressionToMatch) =>
        gen.lt(lit.value)

      case expressions.LessThan(ex, lit: Literal) if ex.semanticEquals(expressionToMatch) =>
        gen.lt(lit.value)

      case expressions.EqualTo(ex, lit: Literal) if ex.semanticEquals(expressionToMatch) =>
        val lt = gen.lt(lit.value)
        val gt = gen.gt(lit.value)
        expressions.And(lt, gt)

      case expressions.Not(expressions.EqualTo(ex, _)) if ex.semanticEquals(expressionToMatch) =>
        expressions.Literal.TrueLiteral

      case expressions.GreaterThanOrEqual(lit: Literal, ex) if ex.semanticEquals(expressionToMatch) =>
        gen.lt(lit.value)

      case expressions.GreaterThan(lit: Literal, ex) if ex.semanticEquals(expressionToMatch) =>
        gen.lt(lit.value)

      case expressions.LessThanOrEqual(lit: Literal, ex) if ex.semanticEquals(expressionToMatch) =>
        gen.gt(lit.value)

      case expressions.LessThan(lit: Literal, ex) if ex.semanticEquals(expressionToMatch) =>
        gen.gt(lit.value)

      case expressions.EqualTo(lit: Literal, ex) if ex.semanticEquals(expressionToMatch) =>
        val lt = gen.lt(lit.value)
        val gt = gen.gt(lit.value)
        expressions.And(lt, gt)

      case expressions.Not(expressions.EqualTo(_, ex)) if ex.semanticEquals(expressionToMatch) =>
        expressions.Literal.TrueLiteral
    }
  }
}
