package jotting.simple

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import jotting.Jotting._
import jotting.{RegimeJdbcHelper, RegimeTimeHelper}

object Job4 {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("RegimeTimeHelper test cases")
      .getOrCreate()

    val config = ConfigFactory.load(configFile).getConfig("job")
    val conn   = Conn(config)

    args.toList match {
      case "mockData" :: _ =>
        mockingDate(conn)
      case "insertFromLastUpdateTime" :: _ =>
        runJdbcDatasetUpdateByLastTime(conn)
      case _ =>
        throw new Exception("Argument is required")
    }

    spark.stop()
  }

  val sourceTable = "job4source"
  val targetTable = "job4target"
  val columns     = Seq("date", "symbol", "price")
  val timeColumn  = "date"

  private def mockingDate(conn: Conn)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val c2d = to_date($"date", "yyyy-MM-dd")

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

    val helper = RegimeJdbcHelper(conn)

    helper.saveTable(sourceDf, sourceTable, "overwrite")
    helper.saveTable(targetDf, targetTable, "overwrite")
  }

  private def runJdbcDatasetUpdateByLastTime(conn: Conn)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // TODO
  }
}
