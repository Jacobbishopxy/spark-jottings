package jotting.simple

import java.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object Job1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("GroupBy Test")
      .getOrCreate()

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

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

    spark.stop()
  }
}
