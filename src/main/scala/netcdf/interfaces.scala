package netcdf

import org.apache.hadoop.fs.Path
import ucar.ma2.{Array => NetCDFArray, Index => NetCDFIndex, Section => NetCDFSection}
import ucar.nc2.{NetcdfFile => NetCDFFile}

import scala.collection.mutable


trait NeighborSearch extends Serializable {
  type In
  type Out

  val inClass: Class[In]
  val outClass: Class[Out]

  def neighbors(x: In): Seq[Out]
  def neighborsDistinct(x: In): Seq[Out] = neighbors(x).distinct
}

trait MonotonicSequence extends Serializable { this: NeighborSearch =>

  // CONTRACT:

  // hasNeighbors(x)  =>  nextSmallerNeighbor(x) <= nextLargerNeighbor(x)

  // if isOrderReversing == false
  // for every value x the following conditions have to hold
  // hasNeighbors(x) && z <= x  =>  neighbors(z) <= nextLargerNeighbor(x)
  // hasNeighbors(x) && z >= x  =>  neighbors(z) >= nextSmallerNeighbor(x)
  // (this essentially means the input sequence as well as the sequence of neighbors have to be monotonic)

  // if isOrderReversing == true
  // for every value x the following conditions have to hold
  // hasNeighbors(x) && z <= x  =>  neighbors(z) >= nextSmallerNeighbor(x)
  // hasNeighbors(x) && z >= x  =>  neighbors(z) <= nextLargerNeighbor(x)
  // (this is used to map descending sequences to ascending indices)

  def isOrderReversing: Boolean

  def hasNeighbors(x: In): Boolean

  def nextSmallerNeighbor(x: In): Out
  def nextLargerNeighbor(x: In): Out

  def neighbors(x: In): Seq[Out] = if (hasNeighbors(x)) {
    Seq(nextSmallerNeighbor(x), nextLargerNeighbor(x))
  } else {
    Seq()
  }
}

trait WrappingMonotonicSequence extends Serializable { this: NeighborSearch =>

  // CONTRACT:

  // !isOverhang(x) =>
  //   nextSmallerNeighbor(x) <= nextLargerNeighbor(x)
  // (non-overhang neighbors need to be monotonic)

  // if isOrderReversing == false
  // for every value x where !isMaxSideOverhang(x) the following conditions have to hold
  // z <= x && hasMinSideOverhang =>
  //   for each neighbor n of z
  //     either n <= nextLargerNeighbor(x)
  //     or     n == largestPossibleNeighbor             (due to wraparound)
  //                                                     (any z that has a wrapping neighbor is called "overhang")
  // z <= x && !hasMinSideOverhang =>
  //   neighbors(z) <= nextLargerNeighbor(x)             (no wraparound in this case)
  // (if isMaxSideOverhang(x) is true, the next larger neighbor would wrap around)

  // all other cases work analogously

  def isOrderReversing: Boolean

  def smallestPossibleNeighbor: Out
  def largestPossibleNeighbor: Out

  def hasMinSideOverhang: Boolean = true
  def hasMaxSideOverhang: Boolean = true

  def isMinSideOverhang(x: In): Boolean
  def isMaxSideOverhang(x: In): Boolean

  def isOverhang(x: In): Boolean = isMinSideOverhang(x) || isMaxSideOverhang(x)

  def nextSmallerNeighbor(x: In): Out
  def nextLargerNeighbor(x: In): Out

  def neighbors(x: In): Seq[Out] = if (isOverhang(x)) {
    Seq(largestPossibleNeighbor, smallestPossibleNeighbor)
  } else {
    Seq(nextSmallerNeighbor(x), nextLargerNeighbor(x))
  }
}


case class Dimension(name: String, extent: Int)


sealed trait RangeFilter extends Serializable {
  def column: String
  def value: Any
}
case class GreaterThan(override val column: String, override val value: Any) extends RangeFilter
case class GreaterThanOrEqual(override val column: String, override val value: Any) extends RangeFilter
case class LessThan(override val column: String, override val value: Any) extends RangeFilter
case class LessThanOrEqual(override val column: String, override val value: Any) extends RangeFilter


trait FilterRewrite extends Serializable {
  def apply(filter: RangeFilter): RangeFilter
}


case class BlockBounds(filters: Seq[RangeFilter])


trait MultidimensionalArrayExtractor extends Serializable {
  def apply(info: Any, array: NetCDFArray, index: NetCDFIndex): AnyRef
}


trait ArrayExtractor extends Serializable {
  def apply(info: Any, array: NetCDFArray, index: Int): AnyRef
}


trait IndexTransform extends Serializable {
  def apply(info: Any, index: Int): AnyRef
}


trait PrecomputePartitionInfo extends Serializable {
  def apply(info: Any, path: Path, file: NetCDFFile): Any
}


trait BuildBlocks extends Serializable {
  def apply(info: Any, path: Path, file: NetCDFFile): Seq[PhysicalNetCDFBlock]
}


// this is an abstract representation on how to interpret each column
sealed trait NetCDFColumn extends Serializable
case class DataColumn(name: String, extractor: MultidimensionalArrayExtractor) extends NetCDFColumn {
  override def toString: String = s"DataColumn($name)"
}
case class DimensionColumn(name: String, dimension: Int, extractor: ArrayExtractor) extends NetCDFColumn {
  override def toString: String = s"DimensionColumn($name, $dimension)"
}
case class DimensionIndexColumn(dimension: Int) extends NetCDFColumn {
  override def toString: String = s"DimensionIndexColumn($dimension)"
}
case class DimensionIndexColumnEx(dimension: Int, transform: IndexTransform) extends NetCDFColumn {
  override def toString: String = s"DimensionIndexColumnEx($dimension)"
}
case object FileIdColumn extends NetCDFColumn
case object FileNameColumn extends NetCDFColumn


// convention: last dimension is the fastest varying dimension (this is mandatory for PhysicalNetCDFBlock)
case class LogicalNetCDFBlock(origin: Seq[Int], extent: Seq[Int]) extends Serializable {
  val count: Int = extent.product
  val dims: Int = origin.size

  require(origin.size == extent.size, "Contradictory dimension count")
}


// contract: last dimension is the fastest varying dimension
case class PhysicalNetCDFBlock(origin: Seq[Int], extent: Seq[Int]) extends Serializable {
  val count: Int = extent.product
  val dims: Int = origin.size

  require(origin.size == extent.size, "Contradictory dimension count")
  require(count > 0, "Cannot have empty physical blocks")
  require(extent.forall(_ > 0), "Block extent must be positive")

  def toSection: NetCDFSection = new NetCDFSection(origin.toArray, extent.toArray)
  def dimSection(dimension: Int): NetCDFSection = new NetCDFSection(Array(origin(dimension)), Array(extent(dimension)))
  def makeIndexWithoutOffset(): NetCDFIndex = NetCDFIndex.factory(extent.toArray)

  override def toString: String = {
    val ranges = origin.zip(extent).map{ case (o, e) => s"$o-${o+e-1}" }.mkString(",")
    s"PhysicalBlock($ranges)"
  }
}
