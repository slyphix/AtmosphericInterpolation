package netcdf

import utils._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import ucar.nc2.Variable

import scala.collection.mutable


// this is the internal representation once a file has been opened
private[netcdf] sealed trait NetCDFLookup extends Serializable
private[netcdf] case class NonDimensionVariable(variable: Variable, extractor: MultidimensionalArrayExtractor) extends NetCDFLookup
private[netcdf] case class DimensionVariable(dimension: Int, variable: Variable, extractor: ArrayExtractor) extends NetCDFLookup
private[netcdf] case class DimensionIndex(dimension: Int, transform: IndexTransform) extends NetCDFLookup
private[netcdf] case class FileId(id: Int) extends NetCDFLookup
private[netcdf] case class FileName(name: String) extends NetCDFLookup


// this is a mutable row implementation to reduce the number of object allocations and data copies
private[netcdf] class GenericReusableBufferLookupRow(override val length: Int) extends Row {
  private var rowIndex: Int = _
  private var buffer: IndexedSeq[AnyRef] = _

  def setRowIndex(rowIndex: Int): Unit = {
    this.rowIndex = rowIndex
  }

  def setBuffer(buffer: mutable.IndexedSeq[AnyRef]): Unit = {
    this.buffer = buffer
  }

  override def get(i: Int): Any = buffer(rowIndex * length + i)

  override def copy(): Row = Row.fromSeq((0 until length).map(get))
}


// corresponds to a single file in a NetCDF dataset
private[netcdf] class GenericNetCDFPartition(
    override val index: Int,
    val path: Path,
    val info: Any
) extends Partition {

  override def hashCode(): Int = index

  override def equals(obj: Any): Boolean = obj match {
    case p: GenericNetCDFPartition => index == p.index && path == p.path
    case _ => false
  }
}


// handles low-level operations like mapping files to machines and data extraction
class GenericNetCDFRDD(
  session: SparkSession,
  protected val files: Seq[Path],
  protected val columns: Seq[NetCDFColumn],
  protected val dimensions: Seq[String],
  protected val precomputePartitionInfo: Option[PrecomputePartitionInfo] = None,
  protected val buildBlocks: Option[BuildBlocks] = None,
  @transient protected val fileInfo: Option[Seq[Any]] = None
) extends RDD[Row](session.sparkContext, Nil) {

  // this is constant for now, but it should rather depend on the number of columns and the size of their respective data types
  protected val MAX_ELEMENTS_PER_BLOCK = 1000000

  private lazy val debugInfo: String =
      s"""GenericNetCDFRDD
         |  for $files
         |  with columns $columns
         |  with info $fileInfo""".stripMargin
  // println(debugInfo)

  private def partitionBlockAlongDimension(block: PhysicalNetCDFBlock, dimension: Int, partitionSize: Int): Seq[PhysicalNetCDFBlock] = {
    val extent = block.extent(dimension)
    val numPartitions = sdiv(extent, partitionSize)
    (0 until numPartitions).map(i => i * partitionSize).map{ ofst =>
      PhysicalNetCDFBlock(
        block.origin.updated(dimension, block.origin(dimension) + ofst),
        block.extent.updated(dimension, partitionSize.min(extent - ofst))
      )
    }
  }

  private def partitionBlockToMaxSize(maxElementsPerBlock: Int, block: PhysicalNetCDFBlock): Seq[PhysicalNetCDFBlock] = {
    // order by descending extent, but always put the fastest varying dimension last
    val (reorderedExtents, reorderedDimensions) = {
      val rest :+ fastestVaryingDimension = block.extent.zipWithIndex
      val sortedRest = rest.sortBy(_._1)(Ordering[Int].reverse)
      val reordered = sortedRest :+ fastestVaryingDimension
      reordered.unzip
    }

    val cumulativeExtents = reorderedExtents.scanRight(1)(_ * _).tail
    val splitIndex = cumulativeExtents.indexWhere(_ <= maxElementsPerBlock)
    val splitDimension = reorderedDimensions(splitIndex)

    // fully patition all dimensions that are too large
    val fullySplit = reorderedDimensions.take(splitIndex).foldLeft(Seq(block)){ (blocks, dim) =>
      blocks.flatMap(b => partitionBlockAlongDimension(b, dim, 1))
    }

    // block-partition first dimension that is small enough
    val blocksPerPartition = maxElementsPerBlock / cumulativeExtents(splitIndex)
    fullySplit.flatMap(b => partitionBlockAlongDimension(b, splitDimension, blocksPerPartition))
  }

  private val hadoopConfigCache = new Configuration(sparkContext.hadoopConfiguration) with Serializable

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val iter: Iterator[Row] = new Iterator[Row] {

      private val partition = split.asInstanceOf[GenericNetCDFPartition]

      private var file = openHDFSFile(partition.path)(hadoopConfigCache)

      private val precomputedInfo = precomputePartitionInfo match {
        case Some(computeInfo) => computeInfo(partition.info, partition.path, file)
        case None => partition.info
      }

      private val blocks = {
        val rawBlocks = buildBlocks match {
          case Some(mapBlocks) =>
            mapBlocks(precomputedInfo, partition.path, file)
          case None =>
            val offsets = Array.fill(dimensions.size)(0)
            val sizes = dimensions.map(file.findDimension).map(_.getLength)
            Seq(PhysicalNetCDFBlock(offsets, sizes))
        }
        rawBlocks.toIterator.flatMap(b => partitionBlockToMaxSize(MAX_ELEMENTS_PER_BLOCK, b))
      }

      private val variables: Seq[NetCDFLookup] = {
        def findVariable(name: String) =
          Option(file.findVariable(name)).getOrElse(throw new IllegalStateException(s"No variable named $name"))

        columns.map {
          case FileIdColumn => FileId(partition.index)
          case FileNameColumn => FileName(partition.path.toString)

          case DimensionIndexColumn(dim) => DimensionIndex(dim, (info, index) => index.asInstanceOf[AnyRef])
          case DimensionIndexColumnEx(dim, transform) => DimensionIndex(dim, transform)

          case DataColumn(name, extractor) =>
            val variable = findVariable(name)
            NonDimensionVariable(variable, extractor)

          case DimensionColumn(name, dim, extractor) =>
            val variable = findVariable(name)
            DimensionVariable(dim, variable, extractor)
        }
      }

      if (blocks.isEmpty)
        close()

      private val row = new GenericReusableBufferLookupRow(variables.size)

      private var buffer: mutable.ArrayBuffer[AnyRef] = _
      private var rowIndex: Int = 0
      private var rowsInBuffer: Int = _

      private def rebuffer(): Unit = {
        if (!blocks.hasNext) return

        val currentBlock = blocks.next()

        rowsInBuffer = currentBlock.count
        buffer = mutable.ArrayBuffer.fill[AnyRef](columns.size * rowsInBuffer)(null)

        for ((variable, vi) <- variables.zipWithIndex) {
          // these are the n-dimensional array indices required to index our block
          val indices = Iterator.iterate(currentBlock.makeIndexWithoutOffset()){ index =>
            index.incr()
            index
          }.take(rowsInBuffer)

          // write data column-wise to avoid pattern matching for every row
          variable match {
            case NonDimensionVariable(variable, extractor) =>
              val contents = variable.read(currentBlock.toSection)
              for (index <- indices)
                buffer(index.currentElement() * columns.size + vi) = extractor(precomputedInfo, contents, index)
            case DimensionVariable(dim, variable, extractor) =>
              val contents = variable.read(currentBlock.dimSection(dim))
              val extracted = (0 until contents.getShape()(0)).map(i => extractor(precomputedInfo, contents, i))
              for (index <- indices)
                buffer(index.currentElement() * columns.size + vi) = extracted(index.getCurrentCounter()(dim))
            case DimensionIndex(dim, transform) =>
              val origin = currentBlock.origin(dim)
              val extent = currentBlock.extent(dim)
              val transformed = origin.until(origin + extent).map(transform(precomputedInfo, _))
              for (index <- indices)
                buffer(index.currentElement() * columns.size + vi) = transformed(index.getCurrentCounter()(dim))
            case FileId(value) =>
              for (i <- 0 until rowsInBuffer)
                buffer(i * columns.size + vi) = value.asInstanceOf[AnyRef]
            case FileName(value) =>
              for (i <- 0 until rowsInBuffer)
                buffer(i * columns.size + vi) = value.asInstanceOf[AnyRef]
          }
        }
        rowIndex = 0
        row.setBuffer(buffer)
      }

      override def hasNext: Boolean = rowIndex < rowsInBuffer || blocks.hasNext

      override def next(): Row = {
        if (!hasNext)
          throw new NoSuchElementException("No more rows in this NetCDF file.")

        if (rowIndex >= rowsInBuffer) {
          rebuffer()
        }

        row.setRowIndex(rowIndex)
        rowIndex += 1

        if (!hasNext)
          close()

        row
      }

      context.addTaskCompletionListener[Unit] { context =>
        close()
      }

      private def close(): Unit = {
        if (file == null)
          return
        file.close()
        file = null
      }
    }
    new InterruptibleIterator(context, iter)
  }

  override protected val getPartitions: Array[Partition] = {
    val inf = fileInfo.getOrElse {
      Seq.fill(files.size)(null)
    }
    files.zip(inf).zipWithIndex.map { case ((path, info), index) =>
      new GenericNetCDFPartition(index, path, info)
    }.toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val path = split.asInstanceOf[GenericNetCDFPartition].path
    val conf = session.sessionState.newHadoopConf()
    val fs: FileSystem = path.getFileSystem(conf)
    // files are never split, so any part of the file resides on the same machine
    val locations = fs.getFileBlockLocations(path, 0, 1).flatMap(_.getHosts)
    locations
  }
}

