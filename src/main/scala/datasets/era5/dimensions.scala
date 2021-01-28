package datasets.era5

import java.time.Instant
import java.time.temporal.ChronoUnit

import netcdf.{ArrayExtractor, Dimension, DimensionColumn, DimensionIndexColumn, DimensionIndexColumnEx, FilterRewrite, LogicalNetCDFBlock, MonotonicSequence, NetCDFColumn, WrappingMonotonicSequence}
import netcdf.filters.{AscendingSequenceFilterRewrite, DescendingSequenceFilterRewrite}
import netcdf.utils.{AbstractNeighborSearch, bisectLeft, bisectRight, makeDimensionExtractor}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, TimestampType}

import ucar.ma2.{DataType => NetCDFType}


trait ERA5Dimension extends Serializable {
  def name: String
  def make(dim: Int): ERA5DimensionInfo
}


trait ERA5DimensionInfo extends Serializable {
  def fields: Seq[StructField]
  def columns: Seq[NetCDFColumn]
  def rewrites: Seq[(String, FilterRewrite)]
  def indexDimension: Dimension
  def rewriteBlocks: Option[BlockRewrite] = None
}


object ERA5StandardTimeDimension extends ERA5Dimension {
  override def name: String = "time"

  override def make(dim: Int): ERA5DimensionInfo = new ERA5DimensionInfo {

    // time column can be of type double despite only storing integral values
    private val castToIntExtractor: ArrayExtractor = { (info, array, index) =>
      array.getInt(index).asInstanceOf[AnyRef]
    }

    private val timeExtractor: ArrayExtractor = { (info, array, index) =>
      val time = array.getLong(index)
      TIME_OFFSET.plus(time, ChronoUnit.HOURS)
    }

    override def fields: Seq[StructField] = Seq(
      StructField("time_index", IntegerType, nullable = false),
      StructField("time", TimestampType, nullable = false))

    override def columns: Seq[NetCDFColumn] = Seq(
      DimensionColumn("time", dim, castToIntExtractor),
      DimensionColumn("time", dim, timeExtractor))

    override def rewrites: Seq[(String, FilterRewrite)] = Seq(
      "time" -> new ERA5InstantToIndex("time_index"))

    override def indexDimension: Dimension = Dimension("time_index", Int.MaxValue)
  }
}


object ERA5StandardLevelDimension extends ERA5PartialLevelDimension(0 to 137)

object ERA5PartialLevelDimension {
  def apply(levels: IndexedSeq[Int]): ERA5PartialLevelDimension = new ERA5PartialLevelDimension(levels)
}

private[era5] class ERA5PartialLevelDimension(levels: IndexedSeq[Int]) extends ERA5Dimension {
  override def name: String = "level"

  override def make(dim: Int): ERA5DimensionInfo = new ERA5DimensionInfo {
    override def fields: Seq[StructField] = Seq(
      StructField("level_index", IntegerType, nullable = false),
      StructField("level", IntegerType, nullable = false))

    override def columns: Seq[NetCDFColumn] = Seq(
      DimensionIndexColumn(dim),
      DimensionColumn("level", dim, makeDimensionExtractor(NetCDFType.INT)))

    override def rewrites: Seq[(String, FilterRewrite)] = Seq(
      "level" -> new AscendingSequenceFilterRewrite[Int]("level_index", levels))

    override def indexDimension: Dimension = Dimension("level_index", levels.size)
  }
}


object ERA5StandardLatitudeDimension extends ERA5Dimension {
  override def name: String = "latitude"

  override def make(dim: Int): ERA5DimensionInfo = new ERA5DimensionInfo {
    override def fields: Seq[StructField] = Seq(
      StructField("latitude_index", IntegerType, nullable = false),
      StructField("latitude", DoubleType, nullable = false))

    override def columns: Seq[NetCDFColumn] = Seq(
      DimensionIndexColumn(dim),
      DimensionColumn("latitude", dim, makeDimensionExtractor(NetCDFType.DOUBLE)))

    override def rewrites: Seq[(String, FilterRewrite)] = Seq(
      "latitude" -> new DescendingSequenceFilterRewrite[Double]("latitude_index", LATITUDES))

    override def indexDimension: Dimension = Dimension("latitude_index", 721)
  }
}


object ERA5StandardLongitudeDimension extends ERA5Dimension {
  override def name: String = "longitude"

  override def make(dim: Int): ERA5DimensionInfo = new ERA5DimensionInfo {
    override def fields: Seq[StructField] = Seq(
      StructField("longitude_index", IntegerType, nullable = false),
      StructField("longitude", DoubleType, nullable = false))

    override def columns: Seq[NetCDFColumn] = Seq(
      DimensionIndexColumn(dim),
      DimensionColumn("longitude", dim, makeDimensionExtractor(NetCDFType.DOUBLE)))

    override def rewrites: Seq[(String, FilterRewrite)] = Seq(
      "longitude" -> new AscendingSequenceFilterRewrite[Double]("longitude_index", LONGITUDES))

    override def indexDimension: Dimension = Dimension("longitude_index", 1440)
  }
}


object ERA5OffsetLongitudeDimension extends ERA5Dimension {
  override def name: String = "longitude"

  override def make(dim: Int): ERA5DimensionInfo = new ERA5DimensionInfo {
    override def fields: Seq[StructField] = Seq(
      StructField("longitude_index", IntegerType, nullable = false),
      StructField("longitude", DoubleType, nullable = false))

    private val extractLongitude: ArrayExtractor = { (info, array, index) =>
      val longitude = array.getDouble(index)
      val shifted = if (longitude >= 180) longitude - 360 else longitude
      shifted.asInstanceOf[AnyRef]
    }

    override def columns: Seq[NetCDFColumn] = Seq(
      DimensionIndexColumnEx(dim, (info, index) => ((index + 720) % 1440).asInstanceOf[AnyRef]),
      DimensionColumn("longitude", dim, extractLongitude))

    override def rewrites: Seq[(String, FilterRewrite)] = Seq(
      "longitude" -> new AscendingSequenceFilterRewrite[Double]("longitude_index", LONGITUDES))

    override def indexDimension: Dimension = Dimension("longitude_index", 1440)

    private def shiftBlockCyclic(block: LogicalNetCDFBlock): Seq[LogicalNetCDFBlock] = {
      val shiftedOrigin = block.origin(dim) + 720
      val extent = block.extent(dim)
      if (shiftedOrigin >= 1440) {
        val newBlock = block.copy(origin = block.origin.updated(dim, shiftedOrigin - 1440))
        Seq(newBlock)
      } else if (shiftedOrigin + extent > 1440) {
        val newExtent = 1440 - shiftedOrigin
        val restExtent = extent - newExtent
        val slicedBlock = LogicalNetCDFBlock(block.origin.updated(dim, shiftedOrigin), block.extent.updated(dim, newExtent))
        val newBlock = LogicalNetCDFBlock(block.origin.updated(dim, 0), block.extent.updated(dim, restExtent))
        Seq(slicedBlock, newBlock)
      } else {
        val newBlock = block.copy(origin = block.origin.updated(dim, shiftedOrigin))
        Seq(newBlock)
      }
    }

    override def rewriteBlocks: Option[BlockRewrite] = Some(shiftBlockCyclic)
  }
}


object ERA5TimeIndexNeighborSearch extends ERA5TimeNeighborSearch

class ERA5TimeIndexNeighborSearch extends AbstractNeighborSearch[Instant, Int] with MonotonicSequence {
  override def isOrderReversing: Boolean = false

  override def nextSmallerNeighbor(instant: Instant): Int = {
    val minutesDifference = TIME_OFFSET.until(instant, ChronoUnit.MINUTES)
    val roundedDown = minutesDifference / 60
    roundedDown.toInt
  }
  override def nextLargerNeighbor(instant: Instant): Int = {
    val minutesDifference = TIME_OFFSET.until(instant, ChronoUnit.MINUTES)
    val roundedUp = (minutesDifference + 59) / 60
    roundedUp.toInt
  }

  override def hasNeighbors(x: Instant): Boolean = true
}


object ERA5TimeNeighborSearch extends ERA5TimeNeighborSearch

class ERA5TimeNeighborSearch extends AbstractNeighborSearch[Instant, Instant] with MonotonicSequence {
  override def isOrderReversing: Boolean = false

  override def nextSmallerNeighbor(instant: Instant): Instant = {
    val minutesDifference = TIME_OFFSET.until(instant, ChronoUnit.MINUTES)
    val hoursRoundedDown = minutesDifference / 60
    TIME_OFFSET.plus(hoursRoundedDown, ChronoUnit.HOURS)
  }
  override def nextLargerNeighbor(instant: Instant): Instant = {
    val minutesDifference = TIME_OFFSET.until(instant, ChronoUnit.MINUTES)
    val hoursRoundedUp = (minutesDifference + 59) / 60
    TIME_OFFSET.plus(hoursRoundedUp, ChronoUnit.HOURS)
  }

  override def hasNeighbors(x: Instant): Boolean = true
}


object ERA5LatitudeIndexNeighborSearch extends ERA5LatitudeIndexNeighborSearch

class ERA5LatitudeIndexNeighborSearch extends AbstractNeighborSearch[Double, Int] with MonotonicSequence {
  override def isOrderReversing: Boolean = true

  override def nextSmallerNeighbor(x: Double): Int = bisectLeft(Ordering[Double].gteq)(x, LATITUDES)
  override def nextLargerNeighbor(x: Double): Int = bisectRight(Ordering[Double].lteq)(x, LATITUDES)

  override def hasNeighbors(x: Double): Boolean = x >= -90 && x <= 90
}

object ERA5LatitudeNeighborSearch extends ERA5LatitudeNeighborSearch

class ERA5LatitudeNeighborSearch extends AbstractNeighborSearch[Double, Double] with MonotonicSequence {
  private val indexSearch = ERA5LatitudeIndexNeighborSearch

  override def isOrderReversing: Boolean = false

  override def nextSmallerNeighbor(x: Double): Double = LATITUDES(indexSearch.nextLargerNeighbor(x))
  override def nextLargerNeighbor(x: Double): Double = LATITUDES(indexSearch.nextSmallerNeighbor(x))

  override def hasNeighbors(x: Double): Boolean = x >= -90 && x <= 90
}


object ERA5LongitudeIndexNeighborSearch extends ERA5LongitudeIndexNeighborSearch

class ERA5LongitudeIndexNeighborSearch extends AbstractNeighborSearch[Double, Int] with WrappingMonotonicSequence {
  override def isOrderReversing: Boolean = false

  override def nextSmallerNeighbor(x: Double): Int = bisectLeft(x, LONGITUDES)
  override def nextLargerNeighbor(x: Double): Int = bisectRight(x, LONGITUDES)

  override def smallestPossibleNeighbor: Int = 0
  override def largestPossibleNeighbor: Int = LONGITUDES.size - 1

  override def hasMinSideOverhang: Boolean = false
  override def isMinSideOverhang(x: Double): Boolean = false

  override def hasMaxSideOverhang: Boolean = true
  override def isMaxSideOverhang(x: Double): Boolean = x > LONGITUDES.last
}


object ERA5LongitudeNeighborSearch extends ERA5LongitudeNeighborSearch

class ERA5LongitudeNeighborSearch extends AbstractNeighborSearch[Double, Double] with WrappingMonotonicSequence {
  private val indexSearch = ERA5LongitudeIndexNeighborSearch

  override def isOrderReversing: Boolean = false

  override def nextSmallerNeighbor(x: Double): Double = LONGITUDES(indexSearch.nextSmallerNeighbor(x))
  override def nextLargerNeighbor(x: Double): Double = LONGITUDES(indexSearch.nextLargerNeighbor(x))

  override def smallestPossibleNeighbor: Double = LONGITUDES.head
  override def largestPossibleNeighbor: Double = LONGITUDES.last

  override def hasMinSideOverhang: Boolean = false
  override def isMinSideOverhang(x: Double): Boolean = false

  override def hasMaxSideOverhang: Boolean = true
  override def isMaxSideOverhang(x: Double): Boolean = x > LONGITUDES.last
}
