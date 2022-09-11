package jotting.simple

import java.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object Job1 {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder
      .appName("GroupBy Test")
      .getOrCreate()

    args.toList match {
      case "countingNumbers" :: tail =>
        countingNumbers(tail: _*)
      case "datetimeFormatConversion" :: _ =>
        datetimeFormatConversion()
      case a @ _ => throw new Exception(s"unsupported args: $a")
    }

    spark.stop()
  }

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  private def countingNumbers(args: String*)(implicit spark: SparkSession): Unit = {
    log.info("Setting variables...")

    val numMappers  = if (args.length > 0) args(0).toInt else 2
    val numKVPairs  = if (args.length > 1) args(1).toInt else 1000
    val valSize     = if (args.length > 2) args(2).toInt else 1000
    val numReducers = if (args.length > 3) args(3).toInt else numMappers

    log.info("calc setup...")
    val pairs1 = spark.sparkContext
      .parallelize(0 until numMappers, numMappers)
      .flatMap { p =>
        val ranGen = new Random
        val arr1   = new Array[(Int, Array[Byte])](numKVPairs)
        for (i <- 0 until numKVPairs) {
          val byteArr = new Array[Byte](valSize)
          ranGen.nextBytes(byteArr)
          arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
        }
        arr1
      }
      .cache()
    // Enforce that everything has been calculated and in cache
    pairs1.count()

    val res = pairs1.groupByKey(numReducers).count()
    println(s"res: $res")
    log.info(s"result: $res")
  }

  private def datetimeFormatConversion()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val format = "yyyyMMddHHmmss"

    val data = Seq(
      (20171230000010L, "ax"),
      (20171230000010L, "aj"),
      (20180101000100L, "ax"),
      (20180101000100L, "aj"),
      (20180203001000L, "ax"),
      (20180203001000L, "aj"),
      (20180321000100L, "ax"),
      (20180321000100L, "aj"),
      (20190510000010L, "ax"),
      (20190510000010L, "aj"),
      (20191002000001L, "ax"),
      (20191002000001L, "aj")
    )
    val df = spark.createDataFrame(data).toDF("date", "symbol")

    val res = df.withColumn("datetype_timestamp", to_timestamp($"date".cast("String"), format))
    res.show()
  }
}
