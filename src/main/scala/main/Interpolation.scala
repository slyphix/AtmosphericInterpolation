package main

import java.time.{Instant, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.TimeZone

import org.apache.hadoop.fs.{GlobFilter, Path}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

import datasets.era5._
import datasets.iagos.IAGOSDataset
import netcdf.neighbors.{findNeighbors, neighborExtensions}
import netcdf.utils.{bisectLeft, collectPaths}

object Interpolation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName(Test.getClass.getName)
      .config("spark.sql.datetime.java8API.enabled", "true")
      .withExtensions(neighborExtensions)
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC))

    val era_ml = {
      val paths = collectPaths(spark, new Path("..\\data\\era5\\"), new GlobFilter("*ml*"))
      val dimensions = Seq(
        ERA5StandardTimeDimension,
        ERA5PartialLevelDimension(40 to 137),
        ERA5StandardLatitudeDimension,
        ERA5StandardLongitudeDimension
      )
      ERA5Dataset.fromFiles(spark, paths, dimensions)
    }

    val era_b = {
      val paths = collectPaths(spark, new Path("..\\data\\era5\\"), new GlobFilter("*B*"))
      val dimensions = Seq(
        ERA5StandardTimeDimension,
        ERA5StandardLatitudeDimension,
        ERA5OffsetLongitudeDimension
      )
      ERA5Dataset.fromFiles(spark, paths, dimensions)
    }

    val iagos = {
      val paths = collectPaths(spark, new Path("..\\data\\iagos_reduced\\"))
      //val paths = collectPaths(spark, new Path("..\\data\\iagos_single\\"))
      //val paths = collectPaths(spark, new Path("..\\data\\iagos_2017_core\\2017\\"))
      IAGOSDataset.fromFiles(spark, paths)
        .withColumn("iagos_time", $"UTC_time")
    }

    val timeWeight = udf { (time: Instant, gridTime: Instant) =>
      if (time == gridTime) {
        1
      } else {
        1 - time.until(gridTime, ChronoUnit.SECONDS).abs / 3600.0
      }
    }

    val latWeight = udf { (lat: Double, gridLat: Double) =>
      if (lat == gridLat) {
        1
      } else {
        1 - (gridLat - lat).abs / LATITUDE_DELTA
      }
    }

    val lonWeight = udf { (lon: Double, gridLon: Double) =>
      if (lon == gridLon) {
        1
      } else {
        val dist = (gridLon - lon).abs
        1 - dist.min(360 - dist) / LONGITUDE_DELTA
      }
    }

    val logPressureWeight = udf { (pressure: Double, gridLogPressure: Double, gridLogPressureDelta: Double) =>
      if (gridLogPressureDelta == 0) {
        1
      } else {
        1 - (gridLogPressure - Math.log(pressure)).abs / gridLogPressureDelta
      }
    }

    def levelNeighborsAndLogPressure(pressure: Double, surfacePressure: Double): Array[LevelNeighbors] = {
      val pressures = new IndexedSeq[Double] {
        override def length: Int = 137
        override def apply(idx: Int): Double = computePressureRaw(idx + 1, surfacePressure)
      }
      val left = bisectLeft(pressure.toDouble, pressures) + 1
      val leftPressure = computePressureRaw(left, surfacePressure)
      val leftLogPressure = Math.log(leftPressure)
      if (pressure == leftPressure) {
        // exact hit, one neighbor
        Array(LevelNeighbors(left, 0, 0))
      } else {
        // inexact hit, two neighbors
        val right = (left + 1) min 137
        val rightPressure = computePressureRaw(right, surfacePressure)
        val rightLogPressure = Math.log(rightPressure)
        val delta = (leftLogPressure - rightLogPressure).abs
        Array(LevelNeighbors(left, leftLogPressure, delta), LevelNeighbors(right, rightLogPressure, delta))
      }
    }
    val findLevelNeighbors = udf(levelNeighborsAndLogPressure _)

    val minMaxPressure = era_b
      .crossJoin((40 to 137).toDF("level"))
      .withColumn("level_pressure", computePressure($"level", $"sp"))
      .where($"latitude" < -40.0)
      .groupBy($"level")
      .agg(min($"level_pressure"), max($"level_pressure"))
      .sort($"level")
      .where($"level" === 137)

    //println(minMaxPressure.queryExecution.executedPlan)
    //minMaxPressure.show(100)

    val neigh = iagos
      .withColumn("longitude_neigh", findNeighbors(ERA5LongitudeNeighborSearch, $"lon"))
      .withColumn("latitude_neigh", findNeighbors(ERA5LatitudeNeighborSearch, $"lat"))
      .withColumn("time_neigh", findNeighbors(ERA5TimeNeighborSearch, $"iagos_time"))
    //println(neigh.queryExecution.executedPlan)

    val sp = neigh
      .join(era_b, $"longitude" === $"longitude_neigh" && $"latitude" === $"latitude_neigh" && $"time" === $"time_neigh")
      .withColumn("time_weight", timeWeight($"iagos_time", $"time"))
      // no automatic filter insertion here for some reason
      .withColumn("lat_weight", latWeight($"lat", $"latitude"))
      .withColumn("lon_weight", lonWeight($"lon", $"longitude"))
      .groupBy("file_id", "iagos_time", "lat", "lon")
      .agg(sum($"time_weight" * $"lat_weight" * $"lon_weight" * $"sp") as "sp_inter")
    //println(sp.queryExecution.executedPlan)
    //sp.crossJoin((40 to 137).toDF("level")).sort($"iagos_time", $"level").withColumn("all_levels", computePressure($"level", $"sp_inter")).show(200)

    def interp4d(col: Column): Column = sum($"time_weight" * $"lat_weight" * $"lon_weight" * $"log_pressure_weight" * col)

    val joined = neigh
      .join(sp, Seq("file_id", "iagos_time", "lat", "lon"))
      .withColumn("level_neighbor_info", explode(findLevelNeighbors($"air_press_AC", $"sp_inter")))
      .withColumn("level_neigh", $"level_neighbor_info.level")
      .withColumn("log_pressure", $"level_neighbor_info.logPressure")
      .withColumn("log_pressure_delta", $"level_neighbor_info.logPressureDelta")
      .join(era_ml, $"longitude" === $"longitude_neigh" && $"latitude" === $"latitude_neigh" && $"time" === $"time_neigh" && $"level" === $"level_neigh")

    val interpolated = joined
      .withColumn("time_weight", timeWeight($"iagos_time", $"time"))
      .withColumn("lat_weight", latWeight($"lat", $"latitude"))
      .withColumn("lon_weight", lonWeight($"lon", $"longitude"))
      .withColumn("log_pressure_weight", logPressureWeight($"air_press_AC", $"log_pressure", $"log_pressure_delta"))
      .groupBy("file_id", "iagos_time", "lat", "lon")
      .agg(count(lit(1)) as "neighbors", interp4d($"t") as "t_inter", interp4d($"q") as "q_inter")

    // relative humidity wrt ice
    val rhi = udf { (pressure: Double, specific_humidity: Double, temperature: Double) =>
      // the approximation is taken from Murphy and Koop, 2005.
      val eps: Double = 0.622
      val preal = specific_humidity * (pressure * 100) / (eps + (1 - eps) * specific_humidity)
      val picesat = Math.exp(9.550426 - 5723.265 / temperature + 3.53068 * Math.log(temperature) - 0.00728332 * temperature)
      preal / picesat
    }

    val compare = interpolated
      .join(iagos, Seq("file_id", "iagos_time", "lat", "lon"))
      .select("iagos_time", "lat", "lon", "neighbors", "air_press_AC", "air_temp_AC", "air_temp_P1", "t_inter", "RHL_P1", "RHL_P1_err", "RHL_P1_stat")
      .sort("iagos_time")

    println(compare.schema)
    println(compare.queryExecution.executedPlan)
    //compare.write.csv("iagos_comparison")
  }
}
