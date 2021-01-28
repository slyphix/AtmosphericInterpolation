package netcdf

import utils._

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.SQLContext


class GenericNetCDFRelation(
    override val sqlContext: SQLContext,
    private val location: FileIndex,
    basePath: Path,
    pathFilter: Option[PathFilter]
) extends AbstractNetCDFRelation with AutoSchemaInference {

  protected lazy val _files : Seq[Path] = {
    // relativize paths to base path so the filter is applied the same way regardless of dataset location
    val inputPaths = location.inputFiles.map(new Path(_)).toSeq
    pathFilter match {
      case None => inputPaths
      case Some(filter) =>
        filterPathsRelativized(basePath, inputPaths)(filter.accept)
    }
  }

  override def files: Seq[Path] = _files
}
