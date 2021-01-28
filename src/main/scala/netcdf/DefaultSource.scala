package netcdf

import utils._

import org.apache.hadoop.fs.{GlobFilter, Path}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.{FileStatusCache, InMemoryFileIndex}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

// taken from
// https://github.com/aokolnychyi/spark-custom-datasource-example/blob/master/src/main/scala/com/aokolnychyi/spark/datasource/DefaultSource.scala


object NetCDFOptions {
  val FILENAME_GLOB_PARAM: String = "netcdf.filename.glob"
  val FILENAME_REGEX_PARAM: String = "netcdf.filename.regex"
  val BASE_PATH_PARAM: String = PartitioningAwareFileIndex.BASE_PATH_PARAM
  val FILE_ID_COLUMN_NAME_PARAM: String = "netcdf.column.fileid.name"
  val FILE_ID_COLUMN_ENABLED_PARAM: String = "netcdf.column.fileid.enabled"
  val FILE_NAME_COLUMN_NAME_PARAM: String = "netcdf.column.filename.name"
  val FILE_NAME_COLUMN_ENABLED_PARAM: String = "netcdf.column.filename.enabled"
  val DIMENSION_INDEX_PREFIX_PARAM: String = "netcdf.dimension.index.prefix"
  val DIMENSION_INDEX_SUFFIX_PARAM: String = "netcdf.dimension.index.suffix"
}

class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val location = parameters.getOrElse("path", throw new IllegalArgumentException("No path specified"))
    val path = new Path(location)

    // datasets can contain a huge number of files, so use an appropriate abstraction here
    // automatic caching and parallel evaluation probably decrease execution time
    val fileStatusCache = FileStatusCache.getOrCreate(sqlContext.sparkSession)
    val fileIndex = new InMemoryFileIndex(sqlContext.sparkSession, Seq(path), parameters, None, fileStatusCache)

    // required for path-level partition filters
    val basePath = parameters.get(NetCDFOptions.BASE_PATH_PARAM) match {
      case Some(p) => new Path(p)
      case None =>
        val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        if (fs.getFileStatus(path).isDirectory) {
          path
        } else {
          path.getParent
        }
    }

    val regexFilter = parameters.get(NetCDFOptions.FILENAME_REGEX_PARAM).map(new RegexFilter(_))
    val globFilter = parameters.get(NetCDFOptions.FILENAME_GLOB_PARAM).map(new GlobFilter(_))
    val filter = regexFilter orElse globFilter

    new GenericNetCDFRelation(sqlContext, fileIndex, basePath, filter)
  }
}
