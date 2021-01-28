package main

import scala.reflect.ClassTag

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import netcdf.{MonotonicSequence, NeighborSearch, WrappingMonotonicSequence}
import netcdf.neighbors._
import netcdf.utils._


case class Name(id: Int, name: String, unused: String)
case class Size(id: Int, size: Float)
case class SizePrice(size: Int, price: Int)


class Rnd extends NeighborSearch with MonotonicSequence {
  type In = Double
  type Out = Int

  val inClass: Class[In] = classOf[Double]
  val outClass: Class[Out] = classOf[Int]

  override def isOrderReversing: Boolean = false

  override def nextSmallerNeighbor(x: Double): Int = x.floor.toInt
  override def nextLargerNeighbor(x: Double): Int = x.ceil.toInt

  override def hasNeighbors(x: Double): Boolean = true
}


class Rounding extends AbstractNeighborSearch[Double, Int] with MonotonicSequence {
  override def isOrderReversing: Boolean = false

  override def nextSmallerNeighbor(x: Double): Int = x.floor.toInt
  override def nextLargerNeighbor(x: Double): Int = x.ceil.toInt

  override def hasNeighbors(x: Double): Boolean = true
}

object Rounding extends Rounding


class NextFullDegree extends AbstractNeighborSearch[Float, Float] with WrappingMonotonicSequence {
  override def isOrderReversing: Boolean = false

  override def nextSmallerNeighbor(x: Float): Float = x.floor.toInt
  override def nextLargerNeighbor(x: Float): Float = x.ceil.toInt

  override def smallestPossibleNeighbor: Float = 0f
  override def largestPossibleNeighbor: Float = 359.0f

  override def hasMinSideOverhang: Boolean = false
  override def isMinSideOverhang(x: Float): Boolean = false

  override def isMaxSideOverhang(x: Float): Boolean = x > 359.0f
}

object NextFullDegree extends NextFullDegree


class ReverseDegreeIndex extends AbstractNeighborSearch[Float, Int] with WrappingMonotonicSequence {
  private val grid = IndexedSeq[Float](315.0f, 270.0f, 225.0f, 180.0f, 135.0f, 90.0f, 45.0f, 0.0f)

  override def isOrderReversing: Boolean = true

  override def smallestPossibleNeighbor: Int = 0
  override def largestPossibleNeighbor: Int = 7

  override def hasMinSideOverhang: Boolean = false
  override def isMinSideOverhang(x: Float): Boolean = false

  override def hasMaxSideOverhang: Boolean = true
  override def isMaxSideOverhang(x: Float): Boolean = x > 315.0f

  override def nextSmallerNeighbor(x: Float): Int = bisectLeft(Ordering[Float].gteq)(x, grid)
  override def nextLargerNeighbor(x: Float): Int = bisectRight(Ordering[Float].lteq)(x, grid)
}

object ReverseDegreeIndex extends ReverseDegreeIndex


class AscGridIndexLookup[A : ClassTag](grid: IndexedSeq[A])(implicit ord: Ordering[A]) extends AbstractNeighborSearch[A, Int] with MonotonicSequence {
  import ord._

  override def isOrderReversing: Boolean = false

  override def nextSmallerNeighbor(x: A): Int = bisectLeft(ord.lteq)(x, grid)
  override def nextLargerNeighbor(x: A): Int = bisectRight(ord.gteq)(x, grid)

  override def hasNeighbors(x: A): Boolean = x >= grid.head && x <= grid.last
}


class DescGridIndexLookup[A : ClassTag](grid: IndexedSeq[A])(implicit ord: Ordering[A]) extends AbstractNeighborSearch[A, Int] with MonotonicSequence {
  import ord._

  override def isOrderReversing: Boolean = true

  override def nextSmallerNeighbor(x: A): Int = bisectLeft(ord.gteq)(x, grid)
  override def nextLargerNeighbor(x: A): Int = bisectRight(ord.lteq)(x, grid)

  override def hasNeighbors(x: A): Boolean = x >= grid.last && x <= grid.head
}

case class LevelNeighbors(level: Int, logPressure: Double, logPressureDelta: Double) extends Serializable

object Test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName(Test.getClass.getName)
      .config("spark.sql.datetime.java8API.enabled", "true")
      .withExtensions(neighborExtensions)
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val piecewiseLinear = sc.parallelize(Seq[(Int, Double, Double)](
      (0, 5, 20), (1, 6, 20), (2, 7, 20), (3, 1, 20), (4, 1, 20), (5, 1, 20), (6, 2, 20), (7, 2, 20), (8, 5, 20), (9, 4, 20), (10, 5, 20)
    )).toDF("knot", "v1", "v2")

    val evalPoints = sc.parallelize(Seq[Double](0.2, 0.4, 0.6, 7.2, 7.5, 8.0, 8.2, 9.5))
      .toDF("ep")
      .filter($"ep".between(0, 1) || $"ep" >= 7)

    val weight = udf { (knot: Int, evalPoint: Double) =>
      if (knot == evalPoint) {
        1
      } else {
        1 - (evalPoint - knot).abs
      }
    }

    /*
    val piecewiseLinearInterpolated = evalPoints
      .withColumn("knot", findNeighbors(Rounding, $"ep"))
      .join(piecewiseLinear, "knot")
      .withColumn("w", weight($"knot", $"ep"))
      .groupBy("ep")
      .agg(sum($"w" * $"v1") as "v1_i", sum($"w" * $"v2") as "v2_i")
      .select($"ep", $"v1_i")
      .filter($"ep" < 8.2)
      .sort($"ep")
    println(piecewiseLinearInterpolated.queryExecution.optimizedPlan)
    piecewiseLinearInterpolated.show()
    */

    val p = sc.parallelize(Seq(1.1f, 2.4f, 3.3f, 7.5f, 10.2f, 350.1f, 359.5f)).toDF("x")
      .where($"x" < 170.2f && $"x" > 1.2f)
      //.withColumn("rounded", findNeighbors(Rounding, $"x"))
      //.withColumn("lookup", findNeighbors(new DescGridIndexLookup[Float](IndexedSeq(10.0f, 5.0f, 3.0f, 0.0f)), $"x"))
      //.withColumn("deg", findNeighbors(NextFullDegree, $"x"))
      .withColumn("ndeg", findNeighbors(ReverseDegreeIndex, $"x"))
    println(p.queryExecution.optimizedPlan)
    p.show()
  }
}
