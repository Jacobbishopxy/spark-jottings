package jotting.simple

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_date, max}
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object Job4 extends App {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  val spark = SparkSession
    .builder()
    .appName("TestDateCompareSql")
    .getOrCreate()

  import spark.implicits._

  val sourceTable = "job4source"
  val targetTable = "job4target"
  val columns     = Seq("date", "symbol", "price")
  val timeColumn  = "date"
  val c2d         = to_date($"date", "yyyy-MM-dd")

  val sourceData = Seq(
    ("2020-01-01", "000001", 10),
    ("2020-01-01", "000300", 231),
    ("2020-01-03", "000001", 11),
    ("2020-01-03", "000300", 201),
    ("2020-01-12", "000001", 15),
    ("2020-01-12", "000300", 191),
    ("2020-02-02", "000001", 21),
    ("2020-02-02", "000300", 149),
    ("2020-05-18", "000001", 18),
    ("2020-05-18", "000300", 131),
    ("2020-08-21", "000001", 19),
    ("2020-08-21", "000300", 154)
  )
  val targetData = sourceData.take(4)

  val sourceDf = spark
    .createDataFrame(sourceData)
    .toDF(columns: _*)
    .withColumn(timeColumn, c2d)
  val targetDf = spark
    .createDataFrame(targetData)
    .toDF(columns: _*)
    .withColumn(timeColumn, c2d)

  sourceDf.createOrReplaceTempView("SOURCE_DF")
  targetDf.createOrReplaceTempView("TARGET_DF")

  val resDf = spark
    .sql(s"""
    SELECT if(
      (SELECT max($timeColumn) FROM SOURCE_DF) > (SELECT max($timeColumn) FROM TARGET_DF),
      (SELECT max($timeColumn) FROM TARGET_DF),
      NULL
    )
    """)
    .toDF()

  val res = resDf.first().get(0)
  resDf.show()

  log.info(s"The last updated date is: $res")
}
