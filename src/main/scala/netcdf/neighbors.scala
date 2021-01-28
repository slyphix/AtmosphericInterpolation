package netcdf

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Generator, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, FunctionIdentifier, InternalRow, JavaTypeInference}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, SparkSessionExtensions}


object neighbors {

  def neighborExtensions(e: SparkSessionExtensions): Unit = {
    e.injectOptimizerRule(_ => PushNeighborFilter)

    val id = FunctionIdentifier("find_neighbors")
    val info = new ExpressionInfo(classOf[FindNeighbors].getName, "FindNeighbors")
    def builder(expr: Seq[Expression]): Expression = expr match {
      case Seq(Literal(classNameRaw, StringType), input) =>
        val className = classNameRaw.toString
        val cls = Class.forName(className)
        val search = cls.getDeclaredConstructor().newInstance().asInstanceOf[NeighborSearch]
        FindNeighbors(search, input)
      case _ => throw new IllegalArgumentException("Illegal arguments passed to find_neighbors")
    }
    e.injectFunction((id, info, builder))
  }

  def findNeighbors(neighborSearch: NeighborSearch, col: Column): Column = withExpr {
    FindNeighbors(neighborSearch, col.expr)
  }

  def findNeighbors(neighborSearchClass: Class[NeighborSearch], col: Column): Column =
    findNeighbors(neighborSearchClass.getDeclaredConstructor().newInstance(), col)

  private def withExpr(expression: Expression): Column = new Column(expression)
}

private[netcdf] case class FindNeighbors(search: NeighborSearch, input: Expression) extends UnaryExpression with Generator with Serializable {

  if (search == null)
    throw new NullPointerException(s"${getClass.getName} parameter may not be null")

  val requiredInputType: DataType = {
    val (dataType, nullable) = JavaTypeInference.inferDataType(search.inClass)
    dataType
  }

  val outputType: DataType = {
    val (dataType, nullable) = JavaTypeInference.inferDataType(search.outClass)
    dataType
  }

  val inputConverter: Any => Any = CatalystTypeConverters.createToScalaConverter(requiredInputType)
  val outputConverter: Any => Any = CatalystTypeConverters.createToCatalystConverter(outputType)

  override val elementSchema: StructType = {
    val field = StructField("neighbors", outputType, nullable = false)
    StructType(Seq(field))
  }

  override def child: Expression = input

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val childEval = inputConverter(child.eval(input)).asInstanceOf[search.In]
    val neighbors = search.neighborsDistinct(childEval)
    neighbors.map(value => InternalRow(outputConverter(value)))
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (DataType.equalsStructurally(child.dataType, requiredInputType, ignoreNullability = true)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"${getClass.getName} requires input type $requiredInputType")
    }
  }

  // code generation is not implemented at the moment because the doGenCode method will never be invoked in the first place
  // http://mail-archives.apache.org/mod_mbox/spark-commits/201703.mbox/%3Cc1f83d9ee6354313be289a27175fee71@git.apache.org%3E
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val info = "If you see this message, code generation has finally been re-enabled for generators, please contact the developer. " +
               "For more information on why generators did not support code generation when this extension was created, see " +
               "http://mail-archives.apache.org/mod_mbox/spark-commits/201703.mbox/%3Cc1f83d9ee6354313be289a27175fee71@git.apache.org%3E"
    throw new UnsupportedOperationException(info);

    // ctx.addReferenceObj()
    // val buffer = classOf[mutable.Buffer[_]].getName
    // val rowData = ctx.addMutableState(s"$buffer<InternalRow>", "rows", v => s"$v = $buffer.empty();")

    // ev.copy(code = code"$code")
  }
}
