package netcdf

import utils._

import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter

import scala.collection.{Set, mutable}


object filters {

  private val DEFAULT_MAX_SIZE_IN_FOR_REWRITE: Int = 100

  case class AscendingSequenceFilterRewrite[A](newColumnName: String, seq: IndexedSeq[A])(implicit ord: Ordering[A]) extends FilterRewrite {
    override def apply(filter: RangeFilter): RangeFilter = {
      filter match {
        case GreaterThan(_, other) =>
          // exclusive binary search
          bisectRightOption(ord.gt, other.asInstanceOf[A], seq, seq.last) match {
            case Some(lowerBound) => GreaterThanOrEqual(newColumnName, lowerBound)
            case None             => GreaterThan(newColumnName, seq.length)
          }
        case GreaterThanOrEqual(_, other) =>
          // inclusive binary search
          bisectRightOption(ord.gteq, other.asInstanceOf[A], seq, seq.last) match {
            case Some(lowerBound) => GreaterThanOrEqual(newColumnName, lowerBound)
            case None             => GreaterThan(newColumnName, seq.length)
          }
        case LessThan(_, other) =>
          // exclusive binary search
          bisectLeftOption(ord.lt, other.asInstanceOf[A], seq, seq.head) match {
            case Some(upperBound) => LessThanOrEqual(newColumnName, upperBound)
            case None             => LessThan(newColumnName, 0)
          }
        case LessThanOrEqual(_, other) =>
          // inclusive binary search
          bisectLeftOption(ord.lteq, other.asInstanceOf[A], seq, seq.head) match {
            case Some(upperBound) => LessThanOrEqual(newColumnName, upperBound)
            case None             => LessThan(newColumnName, 0)
          }
      }
    }
  }


  case class DescendingSequenceFilterRewrite[A](newColumnName: String, seq: IndexedSeq[A])(implicit ord: Ordering[A]) extends FilterRewrite {
    override def apply(filter: RangeFilter): RangeFilter = {
      filter match {
        case GreaterThan(_, other) =>
          bisectLeftOption(ord.gt, other.asInstanceOf[A], seq, seq.head) match {
            case Some(upperBound) => LessThanOrEqual(newColumnName, upperBound)
            case None             => LessThan(newColumnName, 0)
          }
        case GreaterThanOrEqual(_, other) =>
          bisectLeftOption(ord.gteq, other.asInstanceOf[A], seq, seq.head) match {
            case Some(upperBound) => LessThanOrEqual(newColumnName, upperBound)
            case None             => LessThan(newColumnName, 0)
          }
        case LessThan(_, other) =>
          bisectRightOption(ord.lt, other.asInstanceOf[A], seq, seq.last) match {
            case Some(lowerBound) => GreaterThanOrEqual(newColumnName, lowerBound)
            case None             => GreaterThan(newColumnName, seq.size)
          }
        case LessThanOrEqual(_, other) =>
          bisectRightOption(ord.lteq, other.asInstanceOf[A], seq, seq.last) match {
            case Some(lowerBound) => GreaterThanOrEqual(newColumnName, lowerBound)
            case None             => GreaterThan(newColumnName, seq.size)
          }
      }
    }
  }

  def emptyClause: Seq[BlockBounds] = Seq(BlockBounds(Seq()))
  def singleClause(f: RangeFilter): Seq[BlockBounds] = Seq(BlockBounds(Seq(f)))

  def rewriteEqual(attribute: String, value: Any) =
    Seq(BlockBounds(Seq(
      GreaterThanOrEqual(attribute, value),
      LessThanOrEqual(attribute, value))))

  def rewriteNotEqual(attribute: String, value: Any) =
    Seq(
      BlockBounds(Seq(LessThan(attribute, value))),
      BlockBounds(Seq(GreaterThan(attribute, value))))

  def canonicalize(
    filter: Filter,
    coordColumns: Set[String],
    maxSizeInForRewrite: Int = DEFAULT_MAX_SIZE_IN_FOR_REWRITE
  ): Traversable[BlockBounds] = filter match {

    // comparisons work as-is
    case sources.LessThan(attribute, value) if coordColumns.contains(attribute) =>
      singleClause { LessThan(attribute, value) }
    case sources.GreaterThan(attribute, value) if coordColumns.contains(attribute) =>
      singleClause { GreaterThan(attribute, value) }
    case sources.LessThanOrEqual(attribute, value) if coordColumns.contains(attribute) =>
      singleClause { LessThanOrEqual(attribute, value) }
    case sources.GreaterThanOrEqual(attribute, value) if coordColumns.contains(attribute) =>
      singleClause { GreaterThanOrEqual(attribute, value) }

    // equalities are rewritten as comparisons
    case sources.EqualTo(attribute, value) if coordColumns.contains(attribute) =>
      rewriteEqual(attribute, value)
    case sources.Not(sources.EqualTo(attribute, value)) if coordColumns.contains(attribute) =>
      rewriteNotEqual(attribute, value)

    // "in" is rewritten as multiple equality checks as long as the value array is not too large
    case sources.In(attribute, values) if coordColumns.contains(attribute) && values.length <= maxSizeInForRewrite =>
      val rewritten = values
        .filter(_ != null)
        .map(v => sources.EqualTo(attribute, v))
        .reduceLeft[Filter](sources.Or(_, _))
      canonicalize(rewritten, coordColumns)
    case sources.Not(sources.In(attribute, values)) if coordColumns.contains(attribute) && values.length <= maxSizeInForRewrite =>
      val rewritten = values
        .filter(_ != null)
        .map(v => sources.Not(sources.EqualTo(attribute, v)))
        .reduceLeft[Filter](sources.And(_, _))
      canonicalize(rewritten, coordColumns)

    // "or" and "and" have to preserve cnf
    case sources.Or(left, right) =>
      val clhs = canonicalize(left, coordColumns)
      val crhs = canonicalize(right, coordColumns)
      clhs ++ crhs
    case sources.And(left, right) =>
      val clhs = canonicalize(left, coordColumns)
      val crhs = canonicalize(right, coordColumns)
      clhs.flatMap { case BlockBounds(lfilters) =>
        crhs.map { case BlockBounds(rfilters) =>
          BlockBounds(lfilters ++ rfilters)
        }
      }

    // since coordinate axes cannot be nullable, this is always false and should never reach pushdown
    case sources.EqualNullSafe(attribute, _) if coordColumns.contains(attribute) =>
      throw new IllegalStateException("Coordinate axes cannot contain null values and as such the optimizer should have eliminated null-safe equal checks at this point.")
    // since coordinate axes cannot be nullable, this is always true and should never reach pushdown
    case sources.Not(sources.EqualNullSafe(attribute, _)) if coordColumns.contains(attribute) =>
      throw new IllegalStateException("Coordinate axes cannot contain null values and as such the optimizer should have eliminated null-safe equal checks at this point.")

    // everything else is unsupported for pushdown, meaning we need to load everything
    case _ => emptyClause
  }

  def unsafeMergeAlongDimension(lhs: LogicalNetCDFBlock, rhs: LogicalNetCDFBlock, dim: Int): LogicalNetCDFBlock = {
    val lower = lhs.origin(dim).min(rhs.origin(dim))
    val upper = (lhs.origin(dim) + lhs.extent(dim)).max(rhs.origin(dim) + rhs.extent(dim))
    LogicalNetCDFBlock(lhs.origin.updated(dim, lower), rhs.extent.updated(dim, upper - lower))
  }

  def tryMergeAlongDimension(lhs: LogicalNetCDFBlock, rhs: LogicalNetCDFBlock, dim: Int): Option[LogicalNetCDFBlock] = {
    if (lhs.origin(dim) <= rhs.origin(dim) + rhs.extent(dim) && rhs.origin(dim) <= lhs.origin(dim) + lhs.extent(dim)) {
      Some(unsafeMergeAlongDimension(lhs, rhs, dim))
    } else {
      None
    }
  }

  // this has been replaced by the new mergeAll implementation
  protected def mergeableDimension(lhs: LogicalNetCDFBlock, rhs: LogicalNetCDFBlock): Option[Int] = {
    if (lhs.dims != rhs.dims)
      throw new IllegalArgumentException("Block dimensions need to match.")
    val size = lhs.dims
    // two blocks are mergable if origin and extent differ in at most one dimension
    val misalignedDimensions = (0 until size).filter(i => lhs.origin(i) != rhs.origin(i))
    val mismatchedDimensions = (0 until size).filter(i => lhs.extent(i) != rhs.extent(i))
    val mergeDimension = (misalignedDimensions, mismatchedDimensions) match {
      case (Seq(i), Seq(j)) if i == j => Some(i)
      case (Seq(i), Seq( )) => Some(i)
      case (Seq( ), Seq(j)) => Some(j)
      case _ => None
    }
    // and if the blocks are not separated by a gap along that dimension
    mergeDimension.filter { i =>
      lhs.origin(i) <= rhs.origin(i) + rhs.extent(i) && rhs.origin(i) <= lhs.origin(i) + lhs.extent(i)
    }
  }

  def hasOverlap(lhs: LogicalNetCDFBlock, rhs: LogicalNetCDFBlock): Boolean = {
    if (lhs.origin.size != rhs.origin.size)
      throw new IllegalArgumentException("Block dimensions need to match.")
    val size = lhs.origin.size
    (0 until size).forall { dim =>
      lhs.origin(dim) < rhs.origin(dim) + rhs.extent(dim) && rhs.origin(dim) < lhs.origin(dim) + lhs.extent(dim)
    }
  }

  def decomposeOverlappingBlock(keep: LogicalNetCDFBlock, decompose: LogicalNetCDFBlock): Traversable[LogicalNetCDFBlock] = {
    if (keep.origin.size != decompose.origin.size)
      throw new IllegalArgumentException("Block dimensions need to match.")
    val size = keep.origin.size

    val base = (decompose, Seq.empty[LogicalNetCDFBlock])
    val (_, otherBlocks) = (0 until size).foldLeft(base) {
      case ((LogicalNetCDFBlock(decompOrigin, decompExtent), buffer), i) =>
        val lower = decompOrigin(i).max(keep.origin(i))
        val upper = (decompOrigin(i) + decompExtent(i)).min(keep.origin(i) + keep.extent(i))
        // the block below lower is not part of the overlap
        val below = if (decompOrigin(i) < lower) {
          val extentBelow = lower - decompOrigin(i)
          Some(LogicalNetCDFBlock(decompOrigin, decompExtent.updated(i, extentBelow)))
        } else {
          None
        }
        // the block above upper is not part of the overlap
        val above = if (decompOrigin(i) + decompExtent(i) > upper) {
          val extentAbove = decompOrigin(i) + decompExtent(i) - upper
          Some(LogicalNetCDFBlock(decompOrigin.updated(i, upper), decompExtent.updated(i, extentAbove)))
        } else {
          None
        }
        val overlap = LogicalNetCDFBlock(decompOrigin.updated(i, lower), decompExtent.updated(i, upper - lower))
        (overlap, buffer ++ Seq(below, above).flatten)
    }
    otherBlocks
  }

  def removeOverlap(blocks: IndexedSeq[LogicalNetCDFBlock]): Traversable[LogicalNetCDFBlock] = {
    val decomposedBlocks = blocks.map(block => Seq(block)).to[mutable.IndexedSeq]
    val intersectingPairs = indexPairs(blocks.size).filter { case (i, j) => hasOverlap(blocks(i), blocks(j)) }
    intersectingPairs.foreach { case (i, j) =>
      val reference = blocks(i)
      decomposedBlocks(j) = decomposedBlocks(j).flatMap { block =>
        if (hasOverlap(reference, block))
          decomposeOverlappingBlock(reference, block)
        else
          Seq(block)
      }
    }
    decomposedBlocks.flatten
  }

  def removeOverlapSorted(blocks: Traversable[LogicalNetCDFBlock]): Traversable[LogicalNetCDFBlock] = {
    val sortedBlocks = blocks.toSeq.sortBy(_.count)(Ordering[Int].reverse).toIndexedSeq
    removeOverlap(sortedBlocks)
  }

  protected case class NetCDFRange(origin: Int, extent: Int)

  protected def extractIndexRange(filters: Traversable[RangeFilter], max: Int): Option[NetCDFRange] = {
    var localMin = 0
    var localMax = max
    for (filter <- filters) {
      filter match {
        case GreaterThan(_, other) =>
          val lowerBound = other.asInstanceOf[Int] + 1
          if (lowerBound > localMax)
            return None
          localMin = localMin max lowerBound
        case GreaterThanOrEqual(_, other) =>
          val lowerBound = other.asInstanceOf[Int]
          if (lowerBound > localMax)
            return None
          localMin = localMin max lowerBound
        case LessThan(_, other) =>
          val upperBound = other.asInstanceOf[Int] - 1
          if (upperBound < localMin)
            return None
          localMax = localMax min upperBound
        case LessThanOrEqual(_, other) =>
          val upperBound = other.asInstanceOf[Int]
          if (upperBound < localMin)
            return None
          localMax = localMax min upperBound
      }
    }
    Some(NetCDFRange(localMin, localMax - localMin + 1))
  }

  def buildBlockFromRanges(ranges: Traversable[NetCDFRange]): Option[LogicalNetCDFBlock] = {
    val (origins, extents) = ranges
      .map { case NetCDFRange(origin, extent) => (origin, extent) }
      .unzip
    val block = LogicalNetCDFBlock(origins.toSeq, extents.toSeq)
    if (block.count == 0)
      None
    else
      Some(block)
  }

  def buildBlocksFromBounds(dimensions: Seq[Dimension], rewrites: Map[String, FilterRewrite], bounds: Traversable[BlockBounds]): Traversable[LogicalNetCDFBlock] = {
    bounds.flatMap { case BlockBounds(blockFilters) =>
      // rewrite non-dimension filters
      val rewritten = blockFilters.map { filter =>
        rewrites.get(filter.column) match {
          case Some(rewrite) => rewrite(filter)
          case None => filter
        }
      }
      // group by dimension
      val boundsByDimension = rewritten.groupBy(_.column)
      // extract ranges along each dimension
      val ranges = dimensions.map { case Dimension(dimName, dimSize) =>
        boundsByDimension.get(dimName) match {
          case Some(bounds) => extractIndexRange(bounds, dimSize - 1).getOrElse(NetCDFRange(0, 0))
          case None => NetCDFRange(0, dimSize)
        }
      }
      // build block from ranges
      buildBlockFromRanges(ranges)
    }
  }

  def convertFiltersToBlocks(dimensions: Seq[Dimension], rewrites: Map[String, FilterRewrite], filters: Traversable[Filter]): Traversable[LogicalNetCDFBlock] = {
    val pushdownSupportDimensions = dimensions.map(_.name).toSet ++ rewrites.keySet
    // convert filters to convex bounds
    val bounds = canonicalize(filters.reduceLeft(sources.And), pushdownSupportDimensions)
    // bounds can be converted to blocks indpendently
    buildBlocksFromBounds(dimensions, rewrites, bounds)
  }

  def convertFiltersNonEmpty(dimensions: Seq[Dimension], rewrites: Map[String, FilterRewrite], filters: Traversable[Filter]): Traversable[LogicalNetCDFBlock] = {
    val blocks = convertFiltersToBlocks(dimensions, rewrites, filters)
    // remove overlap
    findDisjointCover(blocks).toSeq
    // alternative implementation
    mergeAll(removeOverlapSorted(mergeAll(blocks)))
  }

  def convertFilters(dimensions: Seq[Dimension], rewrites: Map[String, FilterRewrite], filters: Traversable[Filter]): Traversable[LogicalNetCDFBlock] = {
    if (filters.isEmpty) {
      val everything = LogicalNetCDFBlock(Array.fill(dimensions.size)(0), dimensions.map(_.extent))
      Seq(everything)
    } else {
      convertFiltersNonEmpty(dimensions, rewrites, filters)
    }
  }

  def unsafeMergeAllAlongDimension(blocks: Traversable[LogicalNetCDFBlock], dim: Int): Traversable[LogicalNetCDFBlock] = {
    if (blocks.isEmpty)
      return Set.empty

    val sortedBlocks = blocks
      .groupBy(_.origin(dim))
      .values
      .map(bs => bs.maxBy(_.extent(dim)))
      .toSeq
      .sortBy(_.origin(dim))
    val firstBlock +: rest = sortedBlocks
    val base = (firstBlock, Set.empty[LogicalNetCDFBlock])
    val (currentBlock, finishedBlocks) = rest.foldLeft(base) { case ((mergeCandidate, fb), block) =>
      tryMergeAlongDimension(mergeCandidate, block, dim) match {
        case Some(newBlock) => (newBlock, fb)
        case None => (block, fb + mergeCandidate)
      }
    }
    finishedBlocks + currentBlock
  }

  def mergeAll(blocks: Traversable[LogicalNetCDFBlock]): Traversable[LogicalNetCDFBlock] = {
    if (blocks.isEmpty)
      return Seq.empty

    (0 until blocks.head.dims).reverse.foldLeft(blocks) { (acc, dim) =>
      blocks
        .groupBy(block => block.origin.patch(dim, Nil, 1) ++ block.extent.patch(dim, Nil, 1))
        .values
        .flatMap(bs => unsafeMergeAllAlongDimension(bs, dim))
    }
  }

  // polynomial time overlap strategy
  // assume last dimension is fastest varying
  protected sealed trait BlockBound {
    def value: Int
    def block: LogicalNetCDFBlock
  }
  protected case class BlockStart(override val value: Int, override val block: LogicalNetCDFBlock) extends BlockBound
  protected case class BlockEnd(override val value: Int, override val block: LogicalNetCDFBlock) extends BlockBound

  def findCoverInternal(blocks: Traversable[LogicalNetCDFBlock], dim: Int, lastDim: Int): Set[LogicalNetCDFBlock] = {
    if (dim == lastDim) {
      unsafeMergeAllAlongDimension(blocks, lastDim).toSet
    } else {
      if (blocks.isEmpty)
        return Set.empty

      // extract block bounds
      val blockStartEnd = blocks.flatMap { block =>
        Seq(BlockStart(block.origin(dim), block), BlockEnd(block.origin(dim) + block.extent(dim), block))
      }.groupBy(_.value)
      val bounds = blockStartEnd.keySet.toSeq.sorted
      val pairwiseBounds = bounds.zip(bounds.tail)

      // active blocks are blocks overlapping the current interval
      // merge candidates are blocks that can be merged in the next iteration if a suitable block is found
      // finished blocks cannot be merged anymore
      val base = (Set.empty[LogicalNetCDFBlock], Set.empty[LogicalNetCDFBlock], Set.empty[LogicalNetCDFBlock])
      val (_, mergeCandidates, finishedBlocks) = pairwiseBounds.foldLeft(base) {
        case ((activeBlocks, mergeCandidates, finishedBlocks), (lower, upper)) =>
          val newActiveBlocks = blockStartEnd(lower).foldLeft(activeBlocks) {
            case (blocks, BlockStart(_, block)) => blocks + block
            case (blocks, BlockEnd(_, block)) => blocks - block
          }
          // slice current dimension to be between upper and lower
          val slicedBlocks = newActiveBlocks.map { case LogicalNetCDFBlock(origin, extent) =>
            LogicalNetCDFBlock(origin.updated(dim, lower), extent.updated(dim, upper - lower))
          }
          val subblocks = findCoverInternal(slicedBlocks, dim + 1, lastDim)
          // group by equal extent and value along all following dimensions to find mergeable blocks
          val mergeable = (mergeCandidates ++ subblocks)
            .groupBy(block => (block.origin.drop(dim + 1), block.extent.drop(dim + 1)))
            .values
            .map(_.toSeq)

          // merge blocks from current iteration with blocks from last iteration if possible
          val (newMergeCandidates, newFinishedBlocks) = mergeable.foldLeft((Set.empty[LogicalNetCDFBlock], finishedBlocks)) {
            case ((mc, fb), Seq(b1, b2)) =>
              // merged blocks are merge candidates for the next iteration
              (mc + unsafeMergeAlongDimension(b1, b2, dim), fb)
            case ((mc, fb), Seq(b)) if subblocks.contains(b) =>
              // new subblocks are merge candidates for the next iteration
              (mc + b, fb)
            case ((mc, fb), Seq(b)) if mergeCandidates.contains(b) =>
              // unmerged candidates cannot be merged in future iterations
              (mc, fb + b)
            case other => throw new IllegalStateException(s"THIS SHOULD NEVER HAPPEN: $other")
          }
          (newActiveBlocks, newMergeCandidates, newFinishedBlocks)
      }
      finishedBlocks ++ mergeCandidates
    }
  }

  def findDisjointCover(blocks: Traversable[LogicalNetCDFBlock]): Set[LogicalNetCDFBlock] = {
    val lastDim = blocks.headOption match {
      case Some(block) => block.dims - 1
      case None => return Set.empty
    }
    findCoverInternal(blocks, 0, lastDim)
  }

  def transformFilterUp(filter: Filter)(rewrite: PartialFunction[Filter, Filter]): Filter = {
    val transformed = filter match {
      case sources.Not(child) => sources.Not(transformFilterUp(child)(rewrite))
      case sources.And(left, right) => sources.And(transformFilterUp(left)(rewrite), transformFilterUp(right)(rewrite))
      case sources.Or(left, right) => sources.Or(transformFilterUp(left)(rewrite), transformFilterUp(right)(rewrite))
      case other => other
    }
    rewrite.applyOrElse(transformed, identity[Filter])
  }

  def transformFilterDown(filter: Filter)(rewrite: PartialFunction[Filter, Filter]): Filter = {
    val rewritten = rewrite.applyOrElse(filter, identity[Filter])
    rewritten match {
      case sources.Not(child) => sources.Not(transformFilterDown(child)(rewrite))
      case sources.And(left, right) => sources.And(transformFilterDown(left)(rewrite), transformFilterDown(right)(rewrite))
      case sources.Or(left, right) => sources.Or(transformFilterDown(left)(rewrite), transformFilterDown(right)(rewrite))
      case other => other
    }
  }
}
