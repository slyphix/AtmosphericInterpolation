package netcdf

import java.nio.ByteBuffer
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.sql.execution.datasources.{FileStatusCache, InMemoryFileIndex}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import ucar.ma2.{DataType => NetCDFType}
import ucar.nc2.{NetcdfFile => NetCDFFile, Variable => NetCDFVariable}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object utils {

  abstract class AbstractNeighborSearch[InT, OutT](implicit inTag: ClassTag[InT], outTag: ClassTag[OutT]) extends NeighborSearch {
    override type In = InT
    override type Out = OutT

    override val inClass: Class[In] = inTag.runtimeClass.asInstanceOf[Class[In]]
    override val outClass: Class[Out] = outTag.runtimeClass.asInstanceOf[Class[Out]]
  }

  def treeReduce[A](agg: (A, A) => A, seq: IndexedSeq[A]): A = {
    def run(index: Int, skip: Int): A = {
      if (skip == 0)
        return seq(index)
      val left = run(index, skip >> 1)
      if (index + skip < seq.size) {
        val right = run(index + skip, skip >> 1)
        agg(left, right)
      } else {
        left
      }
    }
    val skip = Integer.highestOneBit(seq.size)
    run(0, skip)
  }

  case class Interval[A](startIncl: A, endIncl: A)(implicit ord: Ordering[A]) extends Serializable {
    import ord._

    def findOverlap(other: Interval[A]): Interval[A] =
      Interval(ord.max(startIncl, other.startIncl), ord.min(endIncl, other.endIncl))

    def isEmpty: Boolean = startIncl > endIncl
  }


  class IntervalCover[A](intervals: Traversable[Interval[A]])(implicit ord: Ordering[A]) extends Serializable {
    import ord._

    private def tryMerge(lhs: Interval[A], rhs: Interval[A]): Option[Interval[A]] = {
      (lhs, rhs) match {
        case (Interval(ls, le), Interval(rs, re)) if ls <= re && rs <= le => Some(Interval(ord.min(ls, rs), ord.max(le, re)))
        case _ => None
      }
    }

    val minCover: IndexedSeq[Interval[A]] = {
      val sortedIntervals = intervals
        .toSeq
        .filter(i => i.startIncl <= i.endIncl)
        .sortBy(i => (i.startIncl, i.endIncl))(Ordering.Tuple2(ord, ord.reverse))

      if (sortedIntervals.isEmpty) {
        IndexedSeq()
      } else {
        var currentInterval +: rest = sortedIntervals
        val coverIntervals = mutable.Buffer.empty[Interval[A]]

        for (otherInterval <- rest) {
          tryMerge(currentInterval, otherInterval) match {
            case Some(interval) =>
              currentInterval = interval
            case None =>
              coverIntervals += currentInterval
              currentInterval = otherInterval
          }
        }
        coverIntervals :+ currentInterval
      }.toIndexedSeq
    }

    def findOverlap(interval: Interval[A]): Stream[Interval[A]] = {
      if (minCover.isEmpty)
        return Stream.empty
      // find the smallest end index to the right of the interval start
      val searchIndex = bisectRight(interval.startIncl, minCover.map(_.endIncl))
      // intersect intervals until overlap is empty
      (searchIndex until minCover.size).iterator.map(minCover).map(interval.findOverlap).takeWhile(!_.isEmpty).toStream
    }
  }

  class RegexFilter(pattern: String) extends PathFilter {
    private val compiledPattern = Pattern.compile(pattern)

    override def accept(path: Path): Boolean = compiledPattern.matcher(path.toString).matches()
  }

  implicit class NetCDFReader(base: DataFrameReader) {
    def netCDF(path: String): DataFrame = base.format("netcdf").load(path)
  }

  def indexPairs(n: Int): IndexedSeq[(Int, Int)] = for {
    j <- 0 until n
    i <- 0 until j
  } yield (i, j)

  def bisectLeft[A](lt: (A, A) => Boolean)(x: A, seq: IndexedSeq[A]): Int = {
    var skip = Integer.highestOneBit(seq.size)
    var idx = 0
    while (skip > 0) {
      if (idx + skip < seq.size && lt(seq(idx + skip), x))
        idx = idx + skip
      skip >>= 1
    }
    idx
  }

  def bisectLeft[A](x: A, seq: IndexedSeq[A])(implicit ord: Ordering[A]): Int = bisectLeft(ord.lteq)(x, seq)
  def bisectLeftExclusive[A](x: A, seq: IndexedSeq[A])(implicit ord: Ordering[A]): Int = bisectLeft(ord.lt)(x, seq)

  def bisectLeftOption[A](lt: (A, A) => Boolean, x: A, seq: IndexedSeq[A], lowerLimit: A): Option[Int] = {
    if (!lt(lowerLimit, x)) {
      None
    } else {
      Some(bisectLeft(lt)(x, seq))
    }
  }

  def bisectRight[A](gt: (A, A) => Boolean)(x: A, seq: IndexedSeq[A]): Int = {
    var skip = Integer.highestOneBit(seq.size)
    var idx = seq.size - 1
    while (skip > 0) {
      if (idx - skip >= 0 && gt(seq(idx - skip), x))
        idx = idx - skip
      skip >>= 1
    }
    idx
  }

  def bisectRight[A](x: A, seq: IndexedSeq[A])(implicit ord: Ordering[A]): Int = bisectRight(ord.gteq)(x, seq)
  def bisectRightExclusive[A](x: A, seq: IndexedSeq[A])(implicit ord: Ordering[A]): Int = bisectRight(ord.gt)(x, seq)

  def bisectRightOption[A](gt: (A, A) => Boolean, x: A, seq: IndexedSeq[A], upperLimit: A): Option[Int] = {
    if (!gt(upperLimit, x)) {
      None
    } else {
      Some(bisectRight(gt)(x, seq))
    }
  }

  def sdiv[A](dividend: A, divisor: A)(implicit ev: Integral[A]): A = {
    ev.quot(ev.minus(ev.plus(dividend, divisor), ev.fromInt(1)), divisor)
  }

  def relativizePaths(basePath: Path, paths: Seq[Path]): Iterator[Path] = {
    val baseURI = basePath.toUri
    paths.iterator.map { path =>
      val pathURI = path.toUri
      val relative = baseURI.relativize(pathURI)
      val relativePath = new Path(relative)
      relativePath
    }
  }

  def filterPathsRelativized(basePath: Path, paths: Seq[Path])(predicate: Path => Boolean): Seq[Path] = {
    relativizePaths(basePath, paths).filter(predicate).toSeq
  }

  def collectPaths(spark: SparkSession, rootPath: Path, filter: PathFilter = null): Seq[Path] = {
    val fileStatusCache = FileStatusCache.getOrCreate(spark)
    val fileIndex = new InMemoryFileIndex(spark, Seq(rootPath), Map(), None, fileStatusCache)
    val files = fileIndex.inputFiles.map(new Path(_)).toSeq

    Option(filter) match {
      case None => files
      case Some(pathFilter) =>
        val basePath = {
          val fs = rootPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
          if (fs.getFileStatus(rootPath).isDirectory) {
            rootPath
          } else {
            rootPath.getParent
          }
        }
        filterPathsRelativized(basePath, files)(pathFilter.accept)
    }
  }

  def openHDFSFile(path: Path)(implicit conf: Configuration): NetCDFFile = {
    val fsURI = path.getFileSystem(conf).getUri
    val raf = new HDFSRandomAccessFile(fsURI, path, conf)
    NetCDFFile.open(raf, path.toString, null, null)
  }

  def withHDFSFile[A](path: Path)(op: NetCDFFile => A)(implicit conf: Configuration): A = {
    var file: NetCDFFile = null
    try {
      file = openHDFSFile(path)
      op(file)
    } finally {
      if (file != null)
        file.close()
    }
  }

  def makeNonDimensionExtractor(dataType: NetCDFType): MultidimensionalArrayExtractor = {
    val extractor: MultidimensionalArrayExtractor = dataType match {
      case NetCDFType.CHAR      => (info, array, index) => array.getChar(index).toString
      case NetCDFType.STRING    => (info, array, index) => array.getObject(index)
      case NetCDFType.STRUCTURE => (info, array, index) => null
      case NetCDFType.SEQUENCE  => (info, array, index) => null
      case NetCDFType.OPAQUE    => (info, array, index) => array.getObject(index).asInstanceOf[ByteBuffer].array()
      case NetCDFType.OBJECT    => throw new UnsupportedOperationException("Cannot load NetCDFVariables of type OBJECT")
      case NetCDFType.ENUM1     => (info, array, index) => array.getByte(index).asInstanceOf[AnyRef]
      case NetCDFType.ENUM2     => (info, array, index) => array.getShort(index).asInstanceOf[AnyRef]
      case NetCDFType.ENUM4     => (info, array, index) => array.getInt(index).asInstanceOf[AnyRef]
      case NetCDFType.BOOLEAN   => (info, array, index) => array.getBoolean(index).asInstanceOf[AnyRef]
      case NetCDFType.BYTE      => (info, array, index) => array.getByte(index).asInstanceOf[AnyRef]
      case NetCDFType.SHORT     => (info, array, index) => array.getShort(index).asInstanceOf[AnyRef]
      case NetCDFType.INT       => (info, array, index) => array.getInt(index).asInstanceOf[AnyRef]
      case NetCDFType.LONG      => (info, array, index) => array.getLong(index).asInstanceOf[AnyRef]
      case NetCDFType.FLOAT     => (info, array, index) => array.getFloat(index).asInstanceOf[AnyRef]
      case NetCDFType.DOUBLE    => (info, array, index) => array.getDouble(index).asInstanceOf[AnyRef]
    }
    extractor
  }

  def makeDimensionExtractor(dataType: NetCDFType): ArrayExtractor = {
    val extractor: ArrayExtractor = dataType match {
      case NetCDFType.CHAR
         | NetCDFType.STRING
         | NetCDFType.OPAQUE
         | NetCDFType.STRUCTURE
         | NetCDFType.SEQUENCE
         | NetCDFType.OBJECT
         | NetCDFType.ENUM1
         | NetCDFType.ENUM2
         | NetCDFType.ENUM4 =>
        throw new UnsupportedOperationException("Non-numeric type used as index dimension")
      case NetCDFType.BOOLEAN   => (info, array, index) => array.getBoolean(index).asInstanceOf[AnyRef]
      case NetCDFType.BYTE      => (info, array, index) => array.getByte(index).asInstanceOf[AnyRef]
      case NetCDFType.SHORT     => (info, array, index) => array.getShort(index).asInstanceOf[AnyRef]
      case NetCDFType.INT       => (info, array, index) => array.getInt(index).asInstanceOf[AnyRef]
      case NetCDFType.LONG      => (info, array, index) => array.getLong(index).asInstanceOf[AnyRef]
      case NetCDFType.FLOAT     => (info, array, index) => array.getFloat(index).asInstanceOf[AnyRef]
      case NetCDFType.DOUBLE    => (info, array, index) => array.getDouble(index).asInstanceOf[AnyRef]
    }
    extractor
  }

  def extractSchema(file: NetCDFFile): Seq[StructField] = extractSchema(file.getVariables.asScala)

  def extractSchema(variables: Seq[NetCDFVariable]): Seq[StructField] = variables.map(structField)

  def sparkType(variable: NetCDFVariable): DataType =
    variable.getDataType match {
      case NetCDFType.BOOLEAN   => BooleanType
      case NetCDFType.BYTE      => ByteType
      case NetCDFType.CHAR      => StringType
      case NetCDFType.SHORT     => ShortType
      case NetCDFType.INT       => IntegerType
      case NetCDFType.LONG      => LongType
      case NetCDFType.FLOAT     => FloatType
      case NetCDFType.DOUBLE    => DoubleType
      case NetCDFType.STRING    => StringType
      case NetCDFType.ENUM1     => ByteType
      case NetCDFType.ENUM2     => ShortType
      case NetCDFType.ENUM4     => IntegerType
      case NetCDFType.OPAQUE    => ArrayType(ByteType)
      case NetCDFType.STRUCTURE =>
        // unsupported, but theoretícally possible
        NullType
      case NetCDFType.SEQUENCE  =>
        // unsupported, but theoretícally possible
        NullType
      case NetCDFType.OBJECT    =>
        // implementation detail in netcdf
        throw new UnsupportedOperationException("NetCDF type 'OBJECT' is used internally by the NetCDF library and cannot be used in a DataFrame")
    }

  def structField(variable: NetCDFVariable): StructField = {
    val name = variable.getShortName
    val type_ = sparkType(variable)
    val nullable = type_ == NullType
    StructField(name, type_, nullable)
  }

}
