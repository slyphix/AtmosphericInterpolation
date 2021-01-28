package datasets.era5

import java.time.temporal.ChronoUnit
import java.time.Instant

import netcdf._
import netcdf.utils._
import netcdf.filters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.collection.mutable


object ERA5Dataset extends SchemaInferenceHelpers {

  def fromFiles(spark: SparkSession, paths: Seq[Path], dimensionInfo: Seq[ERA5Dimension]): DataFrame = {
    implicit val config: Configuration = spark.sparkContext.hadoopConfiguration

    val path = paths.headOption.getOrElse(throw new IllegalArgumentException("No input files specified for ERA5 dataset."))

    val fields = mutable.Buffer.empty[StructField]
    val columns = mutable.Buffer.empty[NetCDFColumn]
    val rewrites = mutable.Map.empty[String, FilterRewrite]
    val indexDimensions = mutable.Buffer.empty[Dimension]
    val blockRewrites = mutable.Buffer.empty[BlockRewrite]

    withHDFSFile(path) { file =>
      val (dimensions, _, dataVariables) = partitionVariables(file)

      val timeDimension = dimensions.indexOf("time")

      fields += StructField("path", StringType, nullable = false)
      columns += FileNameColumn

      // replace dimensions with ERA5 standard dimensions
      for ((dimName, dim) <- dimensions.zipWithIndex) {
        val info = dimensionInfo.find(_.name == dimName) match {
          case Some(factory) => factory.make(dim)
          case None => throw new IllegalStateException(s"No info found for dimension $dimName")
        }
        fields ++= info.fields
        columns ++= info.columns
        rewrites ++= info.rewrites
        indexDimensions += info.indexDimension
        blockRewrites ++= info.rewriteBlocks
      }

      for (dv <- dataVariables) {
        val variableName = dv.getShortName

        // extract scale and offset
        val offset = dv.findAttribute("add_offset").getNumericValue.asInstanceOf[Double]
        val scale = dv.findAttribute("scale_factor").getNumericValue.asInstanceOf[Double]

        /*
        // DEFERRED DECOMPRESSION
        // not very effective since this needs to be evaluated as late as possible
        // but spark sql always executes projections as early as possible

        // decompression udf
        val decompress = udf { (compressed: Short) =>
          scale * compressed.toDouble + offset
        }
        */

        // extract null-equivalent value
        val missingValue = dv.findAttribute("missing_value").getNumericValue.asInstanceOf[Short]
        val extractor: MultidimensionalArrayExtractor = { (info, array, index) =>
          val rawValue = array.getShort(index)
          if (rawValue == missingValue) {
            null
          } else {
            val decompressed = scale * rawValue.toDouble + offset
            decompressed.asInstanceOf[AnyRef]
          }
        }

        fields += StructField(variableName, DoubleType, nullable = true)
        columns += DataColumn(variableName, extractor)
      }

      val filterToBlock = { (filters: Seq[Filter]) =>
        val blocks = convertFiltersToBlocks(indexDimensions, rewrites.toMap, filters)
        val rewritten = blockRewrites.foldLeft(blocks)((bs, rewrite) => bs.flatMap(rewrite))
        findDisjointCover(rewritten).toSeq
      }

      val relation = new ERA5Relation(spark.sqlContext, paths, StructType(fields), columns, dimensions, timeDimension, filterToBlock)
      spark.baseRelationToDataFrame(relation)
    }
  }
}


private[era5] class ERA5InstantToIndex(newColumn: String) extends FilterRewrite {
  override def apply(filter: RangeFilter): RangeFilter = {
    filter match {
      case GreaterThan(_, other) =>
        val minutesDifference = TIME_OFFSET.until(other.asInstanceOf[Instant], ChronoUnit.MINUTES)
        val theshold = (minutesDifference + 60) / 60
        GreaterThanOrEqual(newColumn, theshold.toInt)
      case GreaterThanOrEqual(_, other) =>
        val minutesDifference = TIME_OFFSET.until(other.asInstanceOf[Instant], ChronoUnit.MINUTES)
        val theshold = (minutesDifference + 59) / 60
        GreaterThanOrEqual(newColumn, theshold.toInt)
      case LessThan(_, other) =>
        val minutesDifference = TIME_OFFSET.until(other.asInstanceOf[Instant], ChronoUnit.MINUTES)
        val theshold = (minutesDifference - 1) / 60
        LessThanOrEqual(newColumn, theshold.toInt)
      case LessThanOrEqual(_, other) =>
        val minutesDifference = TIME_OFFSET.until(other.asInstanceOf[Instant], ChronoUnit.MINUTES)
        val theshold = minutesDifference / 60
        LessThanOrEqual(newColumn, theshold.toInt)
    }
  }
}


private[era5] class ERA5Relation(
  override val sqlContext: SQLContext,
  override val files: Seq[Path],
  override protected val schemaInternal: StructType,
  override protected val columns: Seq[NetCDFColumn],
  override protected val dimensions: Seq[String],
  protected val timeDimensionIndex: Int,
  protected val filterToBlock: Seq[Filter] => Seq[LogicalNetCDFBlock]
) extends AbstractNetCDFRelation with PrunedFilteredScan {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (filters.isEmpty)
      return buildScan(requiredColumns)

    val blocks = filterToBlock(filters)

    val mapBlocks: BuildBlocks = { (info, path, file) =>
      val thisFileTime = file.findVariable("time").read().getLong(0)
      blocks.flatMap { case LogicalNetCDFBlock(origin, extent) =>
        val startTime = origin(timeDimensionIndex)
        val endTime = origin(timeDimensionIndex) + extent(timeDimensionIndex)
        if (thisFileTime >= startTime && thisFileTime < endTime) {
          Seq(PhysicalNetCDFBlock(origin.updated(timeDimensionIndex, 0), extent.updated(timeDimensionIndex, 1)))
        } else {
          Seq()
        }
      }
    }

    val (_, reducedColumns) = prunedSchema(requiredColumns)
    new GenericNetCDFRDD(sqlContext.sparkSession, files, reducedColumns, dimensions, buildBlocks = Some(mapBlocks))
  }
}
