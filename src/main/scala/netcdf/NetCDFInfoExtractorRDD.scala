package netcdf

import utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import ucar.nc2.{NetcdfFile => NetCDFFile}

import scala.reflect.ClassTag


object NetCDFInfoExtractorRDD {
  def apply[A : ClassTag](session: SparkSession, files: Seq[Path])(infoExtractor: (Path, NetCDFFile) => Seq[A]): NetCDFInfoExtractorRDD[A] =
    new NetCDFInfoExtractorRDD[A](session, files, infoExtractor)
}


private[netcdf] class NetCDFInfoExtractorPartition(
  override val index: Int,
  val path: Path
) extends Partition {

  override def hashCode(): Int = index

  override def equals(obj: Any): Boolean = obj match {
    case p: NetCDFInfoExtractorPartition => index == p.index
    case _ => false
  }
}


class NetCDFInfoExtractorRDD[A : ClassTag](
  session: SparkSession,
  protected val files: Seq[Path],
  protected val infoExtractor: (Path, NetCDFFile) => Seq[A]
) extends RDD[A](session.sparkContext, Nil) {

  private val hadoopConfigCache = new Configuration(sparkContext.hadoopConfiguration) with Serializable

  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    val partiton = split.asInstanceOf[NetCDFInfoExtractorPartition]
    withHDFSFile(partiton.path)(file => infoExtractor(partiton.path, file).iterator)(hadoopConfigCache)
  }

  override protected val getPartitions: Array[Partition] = {
    files.zipWithIndex.map { case (path, index) =>
      new NetCDFInfoExtractorPartition(index, path)
    }.toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val path = split.asInstanceOf[NetCDFInfoExtractorPartition].path
    val conf = session.sessionState.newHadoopConf()
    val fs: FileSystem = path.getFileSystem(conf)
    // files are never split, so any part of the file resides on the same machine
    val locations = fs.getFileBlockLocations(path, 0, 1).flatMap(_.getHosts)
    locations
  }
}
