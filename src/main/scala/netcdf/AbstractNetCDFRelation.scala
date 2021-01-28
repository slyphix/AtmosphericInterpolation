package netcdf

import AbstractNetCDFRelation._
import NetCDFOptions._
import utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import ucar.nc2.{Variable, NetcdfFile => NetCDFFile}

import scala.collection.JavaConverters._
import scala.collection.mutable


object AbstractNetCDFRelation {
  val DEFAULT_DIMENSION_INDEX_PREFIX = ""
  val DEFAULT_DIMENSION_INDEX_SUFFIX = "_i"
  val DEFAULT_FILE_ID_COLUMN_ENABLED = true
  val DEFAULT_FILE_NAME_COLUMN_ENABLED = false
  val DEFAULT_FILE_ID_COLUMN_NAME = "debug_file_id"
  val DEFAULT_FILE_NAME_COLUMN_NAME = "debug_file_name"
}


// handles high-level operations like schema extraction and filter preprocessing
abstract class AbstractNetCDFRelation extends BaseRelation with FileRelation with TableScan with PrunedScan with Serializable {

  protected def files: Seq[Path]
  protected def columns: Seq[NetCDFColumn]
  protected def dimensions: Seq[String]
  protected def schemaInternal: StructType

  override def schema: StructType = schemaInternal

  // we do not use InternalRow since its API is unstable
  override def needConversion: Boolean = true

  def precomputePartitionInfo: PrecomputePartitionInfo = null
  def buildBlocks: BuildBlocks = null
  def fileInfo: Seq[Any] = null

  override def buildScan(): RDD[Row] =
    new GenericNetCDFRDD(
      sqlContext.sparkSession,
      files,
      columns,
      dimensions,
      Option(precomputePartitionInfo),
      Option(buildBlocks),
      Option(fileInfo)
    )

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val (_, reducedColumns) = prunedSchema(requiredColumns)
    new GenericNetCDFRDD(
      sqlContext.sparkSession,
      files,
      reducedColumns,
      dimensions,
      Option(precomputePartitionInfo),
      Option(buildBlocks),
      Option(fileInfo)
    )
  }

  protected def prunedSchema(requiredColumns: Array[String]): (StructType, Seq[NetCDFColumn]) = {
    // use field name as key for column lookup
    val columnsByName = schema.zip(columns)
      .map{ case t@(field, _) => field.name -> t }
      .toMap

    // output column order needs to match requiredColumns
    val (reducedFields, reducedColumns) = requiredColumns.map(columnsByName).unzip
    val reducedSchema = StructType(reducedFields)

    (reducedSchema, reducedColumns)
  }

  // required equality check
  // https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/sources/BaseRelation.html
  override def equals(obj: Any): Boolean = obj match {
    case rel: AbstractNetCDFRelation => schema == rel.schema && files == rel.files
    case _ => false
  }

  override def inputFiles: Array[String] = files.map(_.toString).toArray
}


case class NetCDFSchema(
  dimensions: Seq[String],
  dimensionFields: Seq[StructField],
  dimensionColumns: Seq[NetCDFColumn],
  dataFields: Seq[StructField],
  dataColumns: Seq[NetCDFColumn]
) extends Serializable


trait SchemaInferenceHelpers {

  protected def partitionVariables(file: NetCDFFile): (Seq[String], Seq[Variable], Seq[Variable]) = {
    // the order of dimensions can differ between groups and variables!
    val unorderedDimensions = file.getRootGroup.getDimensions.asScala.map(_.getShortName).toSet
    val variables = file.getVariables.asScala
    val nonDimensionVariables = variables.filter(v => !unorderedDimensions.contains(v.getShortName))

    // get correct dimension order from one of the data variables
    val dimensions = nonDimensionVariables.headOption match {
      case Some(nonDimVar) => nonDimVar.getDimensions.asScala.map(_.getShortName)
      case None => unorderedDimensions.toSeq
    }
    val hasConsistentVariableOrder = nonDimensionVariables.forall(v => dimensions == v.getDimensions.asScala.map(_.getShortName))
    if (!hasConsistentVariableOrder)
      throw new IllegalStateException("Inconsistent dimension ordering")

    // requires correct order! also, some dimensions might not have associated variables, so use flatmap
    val dimensionVariables = dimensions.flatMap(name => Option(file.findVariable(name)))
    (dimensions, dimensionVariables, nonDimensionVariables)
  }

  protected def inferSchemaFromFile(file: NetCDFFile): NetCDFSchema = {
    val (dimensions, dimensionVariables, nonDimensionVariables) = partitionVariables(file)

    val dimensionColumns = dimensionVariables.zipWithIndex.map { case (v, i) => DimensionColumn(v.getShortName, i, makeDimensionExtractor(v.getDataType)) }
    val dataColumns = nonDimensionVariables.map(v => DataColumn(v.getShortName, makeNonDimensionExtractor(v.getDataType)))

    NetCDFSchema(
      dimensions,
      extractSchema(dimensionVariables),
      dimensionColumns,
      extractSchema(nonDimensionVariables),
      dataColumns
    )
  }
}


trait AutoSchemaInference extends SchemaInferenceHelpers { this: AbstractNetCDFRelation =>

  protected def composeSchema(file: NetCDFFile): (StructType, Seq[NetCDFColumn], Seq[String]) = {

    val NetCDFSchema(dimensions, dimensionFields, dimensionColumns, dataFields, dataColumns) = inferSchemaFromFile(file)

    val conf = sqlContext.sparkContext.getConf
    val fields = mutable.Buffer.empty[StructField]
    val columns = mutable.Buffer.empty[NetCDFColumn]

    // file id
    if (conf.getBoolean(FILE_ID_COLUMN_ENABLED_PARAM, DEFAULT_FILE_ID_COLUMN_ENABLED)) {
      val fileIdColumnName = conf.get(FILE_ID_COLUMN_NAME_PARAM, DEFAULT_FILE_ID_COLUMN_NAME)
      fields += StructField(fileIdColumnName, IntegerType, nullable = false)
      columns += FileIdColumn
    }

    // file name
    if (conf.getBoolean(FILE_NAME_COLUMN_ENABLED_PARAM, DEFAULT_FILE_NAME_COLUMN_ENABLED)) {
      val fileNameColumnName = conf.get(FILE_NAME_COLUMN_NAME_PARAM, DEFAULT_FILE_NAME_COLUMN_NAME)
      fields += StructField(fileNameColumnName, StringType, nullable = false)
      columns += FileNameColumn
    }

    // dimension index
    val dimensionIndexPrefix = conf.get(DIMENSION_INDEX_PREFIX_PARAM, DEFAULT_DIMENSION_INDEX_PREFIX)
    val dimensionIndexSuffix = conf.get(DIMENSION_INDEX_SUFFIX_PARAM, DEFAULT_DIMENSION_INDEX_SUFFIX)
    fields ++= dimensions.map(d => StructField(dimensionIndexPrefix + d + dimensionIndexSuffix, IntegerType, nullable = false))
    columns ++= dimensions.indices.map(i => DimensionIndexColumn(i))

    // variables
    fields ++= dimensionFields ++ dataFields
    columns ++= dimensionColumns ++ dataColumns

    (StructType(fields), columns, dimensions)
  }

  protected def buildSchema(): (StructType, Seq[NetCDFColumn], Seq[String]) = {
    files.headOption match {
      case None =>
        // no files, no schema!
        (StructType(Seq.empty[StructField]), Seq.empty[NetCDFColumn], Seq.empty[String])
      case Some(path) =>
        implicit val conf: Configuration = sqlContext.sparkContext.hadoopConfiguration
        withHDFSFile(path) { file =>
          composeSchema(file)
        }
    }
  }

  protected override val (schemaInternal, columns, dimensions) = buildSchema()
}
