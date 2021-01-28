package datasets.iagos

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.nio

import netcdf._
import netcdf.utils._
import netcdf.filters._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import swaydb.data.slice.Slice
import swaydb.{Glass, persistent}
import swaydb.serializers.Serializer
import swaydb.serializers.Default._

import scala.util.matching.Regex


private[datasets] case class Info(timeOffset: Instant, envelope: Interval[Instant]) extends Serializable


object IAGOSDataset {
  private val FileOffset: Regex = ".*(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})".r

  implicit private val serializeInfo: Serializer[Info] = new Serializer[Info] {
    override def write(data: Info): Slice[Byte] = Slice
      .ofBytesScala(48)
      .addLong(data.timeOffset.getEpochSecond)
      .addInt(data.timeOffset.getNano)
      .addLong(data.envelope.startIncl.getEpochSecond)
      .addInt(data.envelope.startIncl.getNano)
      .addLong(data.envelope.endIncl.getEpochSecond)
      .addInt(data.envelope.endIncl.getNano)
      .close()

    override def read(slice: Slice[Byte]): Info = {
      val reader = slice.createReader()
      val timeOffset = Instant.ofEpochSecond(reader.readLong(), reader.readInt())
      val start = Instant.ofEpochSecond(reader.readLong(), reader.readInt())
      val end = Instant.ofEpochSecond(reader.readLong(), reader.readInt())
      Info(timeOffset, Interval(start, end))
    }
  }

  implicit private val serializePath: Serializer[Path] = new Serializer[Path] {
    override def write(data: Path): Slice[Byte] = Slice.writeStringUTF8(data.toString)
    override def read(slice: Slice[Byte]): Path = new Path(slice.readString())
  }

  // MAYBE compute envelopes for lat/lon
  // MAYBE split flights into n parts and compute n envelopes
  private def launchGatherInfo(spark: SparkSession, files: Seq[Path]): Seq[(String, Info)] = {
    val infordd = NetCDFInfoExtractorRDD(spark, files){ (path, file) =>
      val timeVariable = file.findVariable("UTC_time")
      val unit = timeVariable.getUnitsString
      val offset = unit match {
        case FileOffset(year, month, day, hour, minute, second) =>
          LocalDateTime.of(year.toInt, month.toInt, day.toInt, hour.toInt, minute.toInt, second.toInt).toInstant(ZoneOffset.UTC)
        case _ =>
          throw new IllegalStateException("Could not extract time offset")
      }
      val variableSize = timeVariable.getDimension(0).getLength
      val lower = offset.plus(timeVariable.read(Array(0), Array(1)).getLong(0), ChronoUnit.SECONDS)
      val upper = offset.plus(timeVariable.read(Array(variableSize - 1), Array(1)).getLong(0), ChronoUnit.SECONDS)
      Seq((path.getName, Info(offset, Interval(lower, upper))))
    }
    infordd.collect()
  }

  def fromFiles(spark: SparkSession, files: Seq[Path]): DataFrame = {
    val infoCache = persistent.Map[String, Info, Nothing, Glass](dir = nio.file.Paths.get("iagos-envelopes"))
    val uncachedFiles = files.filterNot(file => infoCache.contains(file.getName))
    if (uncachedFiles.nonEmpty) {
      infoCache.put(launchGatherInfo(spark, uncachedFiles))
    }
    val info = files.map(file => infoCache.get(file.getName).getOrElse(throw new IllegalStateException("No envelope found in cache. This should never happen.")))

    val relation = new IAGOSRelation(spark.sqlContext, files, info)
    val df = spark.baseRelationToDataFrame(relation)
    val bounds = info.map(i => col("UTC_time").between(i.envelope.startIncl, i.envelope.endIncl)).toIndexedSeq
    if (bounds.isEmpty) {
      df
    } else {
      val boundsFilter = treeReduce[Column](_ || _, bounds)
      df.filter(boundsFilter)
    }
  }
}


private class IAGOSRelation(
  override val sqlContext: SQLContext,
  override val files: Seq[Path],
  override val fileInfo: Seq[Info]
) extends AbstractNetCDFRelation with PrunedFilteredScan {

  override protected val (schemaInternal, columns): (StructType, Seq[NetCDFColumn]) = {
    val timeExtractor: ArrayExtractor = { (info, array, index) =>
      val time = array.getLong(index)
      info.asInstanceOf[Info].timeOffset.plus(time, ChronoUnit.SECONDS)
    }
    val ILLEGAL_VALUE: Double = -9999
    val measurementExtractor: MultidimensionalArrayExtractor = { (info, array, index) =>
      val raw = array.getDouble(index)
      if (raw == ILLEGAL_VALUE) null else raw.asInstanceOf[AnyRef]
    }
    val (fields, columns) = Seq(
      (StructField("file_id",               IntegerType,   nullable = false), FileIdColumn),
      (StructField("path",                  StringType,    nullable = false), FileNameColumn),
      (StructField("UTC_time_i",            IntegerType,   nullable = false), DimensionIndexColumn(0)),
      (StructField("UTC_time",              TimestampType, nullable = false), DimensionColumn("UTC_time", 0, timeExtractor)),
      (StructField("lon",                   DoubleType,    nullable = false), DataColumn("lon",                   measurementExtractor)),
      (StructField("lat",                   DoubleType,    nullable = false), DataColumn("lat",                   measurementExtractor)),
      (StructField("baro_alt_AC",           DoubleType,    nullable = true),  DataColumn("baro_alt_AC",           measurementExtractor)),
      (StructField("baro_alt_AC_val",       DoubleType,    nullable = true),  DataColumn("baro_alt_AC_val",       measurementExtractor)),
      (StructField("radio_alt_AC",          DoubleType,    nullable = true),  DataColumn("radio_alt_AC",          measurementExtractor)),
      (StructField("radio_alt_AC_val",      DoubleType,    nullable = true),  DataColumn("radio_alt_AC_val",      measurementExtractor)),
      (StructField("gps_alt_AC",            DoubleType,    nullable = true),  DataColumn("gps_alt_AC",            measurementExtractor)),
      (StructField("gps_alt_AC_val",        DoubleType,    nullable = true),  DataColumn("gps_alt_AC_val",        measurementExtractor)),
      (StructField("air_press_AC",          DoubleType,    nullable = true),  DataColumn("air_press_AC",          measurementExtractor)),
      (StructField("air_press_AC_val",      DoubleType,    nullable = true),  DataColumn("air_press_AC_val",      measurementExtractor)),
      (StructField("air_speed_AC",          DoubleType,    nullable = true),  DataColumn("air_speed_AC",          measurementExtractor)),
      (StructField("air_speed_AC_val",      DoubleType,    nullable = true),  DataColumn("air_speed_AC_val",      measurementExtractor)),
      (StructField("ground_speed_AC",       DoubleType,    nullable = true),  DataColumn("ground_speed_AC",       measurementExtractor)),
      (StructField("ground_speed_AC_val",   DoubleType,    nullable = true),  DataColumn("ground_speed_AC_val",   measurementExtractor)),
      (StructField("air_temp_AC",           DoubleType,    nullable = true),  DataColumn("air_temp_AC",           measurementExtractor)),
      (StructField("air_temp_AC_val",       DoubleType,    nullable = true),  DataColumn("air_temp_AC_val",       measurementExtractor)),
      (StructField("air_stag_temp_AC",      DoubleType,    nullable = true),  DataColumn("air_stag_temp_AC",      measurementExtractor)),
      (StructField("air_stag_temp_AC_val",  DoubleType,    nullable = true),  DataColumn("air_stag_temp_AC_val",  measurementExtractor)),
      (StructField("wind_dir_AC",           DoubleType,    nullable = true),  DataColumn("wind_dir_AC",           measurementExtractor)),
      (StructField("wind_dir_AC_val",       DoubleType,    nullable = true),  DataColumn("wind_dir_AC_val",       measurementExtractor)),
      (StructField("wind_speed_AC",         DoubleType,    nullable = true),  DataColumn("wind_speed_AC",         measurementExtractor)),
      (StructField("wind_speed_AC_val",     DoubleType,    nullable = true),  DataColumn("wind_speed_AC_val",     measurementExtractor)),
      (StructField("zon_wind_AC",           DoubleType,    nullable = true),  DataColumn("zon_wind_AC",           measurementExtractor)),
      (StructField("zon_wind_AC_val",       DoubleType,    nullable = true),  DataColumn("zon_wind_AC_val",       measurementExtractor)),
      (StructField("mer_wind_AC",           DoubleType,    nullable = true),  DataColumn("mer_wind_AC",           measurementExtractor)),
      (StructField("mer_wind_AC_val",       DoubleType,    nullable = true),  DataColumn("mer_wind_AC_val",       measurementExtractor)),
      (StructField("air_temp_P1",           DoubleType,    nullable = true),  DataColumn("air_temp_P1",           measurementExtractor)),
      (StructField("air_temp_P1_err",       DoubleType,    nullable = true),  DataColumn("air_temp_P1_err",       measurementExtractor)),
      (StructField("air_temp_P1_val",       DoubleType,    nullable = true),  DataColumn("air_temp_P1_val",       measurementExtractor)),
      (StructField("air_temp_P1_stat",      DoubleType,    nullable = true),  DataColumn("air_temp_P1_stat",      measurementExtractor)),
      (StructField("cloud_P1",              DoubleType,    nullable = true),  DataColumn("cloud_P1",              measurementExtractor)),
      (StructField("cloud_P1_err",          DoubleType,    nullable = true),  DataColumn("cloud_P1_err",          measurementExtractor)),
      (StructField("cloud_P1_val",          DoubleType,    nullable = true),  DataColumn("cloud_P1_val",          measurementExtractor)),
      (StructField("cloud_P1_stat",         DoubleType,    nullable = true),  DataColumn("cloud_P1_stat",         measurementExtractor)),
      (StructField("CO_P1",                 DoubleType,    nullable = true),  DataColumn("CO_P1",                 measurementExtractor)),
      (StructField("CO_P1_err",             DoubleType,    nullable = true),  DataColumn("CO_P1_err",             measurementExtractor)),
      (StructField("CO_P1_val",             DoubleType,    nullable = true),  DataColumn("CO_P1_val",             measurementExtractor)),
      (StructField("CO_P1_stat",            DoubleType,    nullable = true),  DataColumn("CO_P1_stat",            measurementExtractor)),
      (StructField("H2O_gas_P1",            DoubleType,    nullable = true),  DataColumn("H2O_gas_P1",            measurementExtractor)),
      (StructField("H2O_gas_P1_err",        DoubleType,    nullable = true),  DataColumn("H2O_gas_P1_err",        measurementExtractor)),
      (StructField("H2O_gas_P1_val",        DoubleType,    nullable = true),  DataColumn("H2O_gas_P1_val",        measurementExtractor)),
      (StructField("H2O_gas_P1_stat",       DoubleType,    nullable = true),  DataColumn("H2O_gas_P1_stat",       measurementExtractor)),
      (StructField("O3_P1",                 DoubleType,    nullable = true),  DataColumn("O3_P1",                 measurementExtractor)),
      (StructField("O3_P1_err",             DoubleType,    nullable = true),  DataColumn("O3_P1_err",             measurementExtractor)),
      (StructField("O3_P1_val",             DoubleType,    nullable = true),  DataColumn("O3_P1_val",             measurementExtractor)),
      (StructField("O3_P1_stat",            DoubleType,    nullable = true),  DataColumn("O3_P1_stat",            measurementExtractor)),
      (StructField("RHL_P1",                DoubleType,    nullable = true),  DataColumn("RHL_P1",                measurementExtractor)),
      (StructField("RHL_P1_err",            DoubleType,    nullable = true),  DataColumn("RHL_P1_err",            measurementExtractor)),
      (StructField("RHL_P1_val",            DoubleType,    nullable = true),  DataColumn("RHL_P1_val",            measurementExtractor)),
      (StructField("RHL_P1_stat",           DoubleType,    nullable = true),  DataColumn("RHL_P1_stat",           measurementExtractor)),
      (StructField("cloud_presence_P1",     DoubleType,    nullable = true),  DataColumn("cloud_presence_P1",     measurementExtractor)),
      (StructField("air_stag_temp_P1",      DoubleType,    nullable = true),  DataColumn("air_stag_temp_P1",      measurementExtractor)),
      (StructField("air_stag_temp_P1_err",  DoubleType,    nullable = true),  DataColumn("air_stag_temp_P1_err",  measurementExtractor)),
      (StructField("air_stag_temp_P1_val",  DoubleType,    nullable = true),  DataColumn("air_stag_temp_P1_val",  measurementExtractor)),
      (StructField("air_stag_temp_P1_stat", DoubleType,    nullable = true),  DataColumn("air_stag_temp_P1_stat", measurementExtractor))
    ).unzip
    (StructType(fields), columns)
  }
  override protected val dimensions: Seq[String] = Seq("UTC_time")

  private def extractInstantInterval(filters: Seq[RangeFilter]): Option[Interval[Instant]] = {
    val ord = Ordering[Instant]
    import ord._

    var localMin = Instant.MIN
    var localMax = Instant.MAX
    for (filter <- filters) {
      filter match {
        case GreaterThan(_, other) =>
          val lowerBound = other.asInstanceOf[Instant].plusNanos(1)
          if (lowerBound > localMax)
            return None
          localMin = ord.max(localMin, lowerBound)
        case GreaterThanOrEqual(_, other) =>
          val lowerBound = other.asInstanceOf[Instant]
          if (lowerBound > localMax)
            return None
          localMin = ord.max(localMin, lowerBound)
        case LessThan(_, other) =>
          val upperBound = other.asInstanceOf[Instant].minusNanos(1)
          if (upperBound < localMin)
            return None
          localMax = ord.min(localMax, upperBound)
        case LessThanOrEqual(_, other) =>
          val upperBound = other.asInstanceOf[Instant]
          if (upperBound < localMin)
            return None
          localMax = ord.min(localMax, upperBound)
      }
    }
    Some(Interval(localMin, localMax))
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (filters.isEmpty)
      return buildScan(requiredColumns)

    val cover = {
      val bounds = canonicalize(filters.reduce(sources.And), Set("UTC_time"))
      val intervals = bounds.flatMap { case BlockBounds(filters) => extractInstantInterval(filters) }
      new IntervalCover[Instant](intervals)
    }

    val (relevantFiles, relevantInfo) = files.zip(fileInfo).filter { case (file, info) =>
      cover.findOverlap(info.envelope).nonEmpty
    }.unzip

    val buildBlocks: BuildBlocks = { (info, path, file) =>
      val Info(timeOffset, envelope) = info.asInstanceOf[Info]
      val grid = file
        .findVariable("UTC_time")
        .read()
        .copyTo1DJavaArray()
        .asInstanceOf[Array[Double]]
        .map(_.toLong)
        .toIndexedSeq

      val overlap = cover.findOverlap(envelope)
      val blocks = overlap.map { case Interval(start, end) =>
        val startSeconds = timeOffset.until(start, ChronoUnit.SECONDS)
        val endSeconds = timeOffset.until(end, ChronoUnit.SECONDS)
        val startIndex = bisectRight(startSeconds, grid)
        val endIndex = bisectLeft(endSeconds, grid)
        PhysicalNetCDFBlock(Array(startIndex), Array(endIndex - startIndex + 1))
      }
      blocks
    }

    if (relevantFiles.nonEmpty) {
      val (_, reducedColumns) = prunedSchema(requiredColumns)
      new GenericNetCDFRDD(
        sqlContext.sparkSession,
        relevantFiles,
        reducedColumns,
        dimensions,
        fileInfo = Some(relevantInfo),
        buildBlocks = Some(buildBlocks)
      )
    } else {
      sqlContext.sparkContext.emptyRDD
    }
  }
}

