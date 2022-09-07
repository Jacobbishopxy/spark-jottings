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
import jotting.MyJdbcUtils

/** Test cases for verifying MyJdbcUtils' correctness
  *
  *   - save
  *   - createUnique
  *   - dropUnique
  *   - createIndex
  *   - dropIndex
  *   - upsert
  *   - delete
  *   - drop
  */
object Job3 {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("MyJdbcUtils test cases")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    log.info("loading config file...")

    val config = ConfigFactory.load(configFile).getConfig("job")
    val conn   = Conn(config)

    log.info("config file loaded: ")
    log.info(conn)

    args.toList match {
      case "save" :: _         => runJdbcDatasetSave(conn)
      case "createUnique" :: _ => runJdbcCreateUnique(conn)
      case "dropUnique" :: _   => runJdbcDropUnique(conn)
      case "createIndex" :: _  => runJdbcCreateIndex(conn)
      case "dropIndex" :: _    => runJdbcDropIndex(conn)
      case "upsert" :: _       => runJdbcDatasetUpsert(conn)
      case "delete" :: _       => runJdbcDatasetDelete(conn)
      case "drop" :: _         => runJdbcDropTable(conn)
      case _                   => throw new Exception("Argument is required")
    }

    spark.stop()
  }

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  val save_to = "job3"

  private def runJdbcDatasetSave(conn: Conn)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    log.info("runJdbcDatasetSave...")

    val data = Seq(
      ("2010-01-23", "Java", 20000),
      ("2010-01-23", "Python", 100000),
      ("2010-01-23", "Scala", 3000),
      ("2015-08-15", "Java", 25000),
      ("2015-08-15", "Python", 150000),
      ("2015-08-15", "Scala", 2000)
    )
    val df = spark
      .createDataFrame(data)
      .toDF("date", "language", "users_count")
      .withColumn("date", to_date($"date", "yyyy-MM-dd"))

    val helper = MyJdbcUtils(conn)

    // save table
    helper.saveTable(df, save_to, "overwrite")

    // add unique constraint
    helper.createUniqueConstraint(
      save_to,
      s"${save_to}_date_language",
      Seq("date", "language")
    )
  }

  private def runJdbcCreateUnique(conn: Conn)(implicit spark: SparkSession): Unit = {
    log.info("runJdbcCreateUnique...")
    MyJdbcUtils(conn).createUniqueConstraint(
      save_to,
      s"${save_to}_date_language",
      Seq("date", "language")
    )
  }

  private def runJdbcDropUnique(conn: Conn)(implicit spark: SparkSession): Unit = {
    log.info("runJdbcDropUnique...")
    MyJdbcUtils(conn).dropUniqueConstraint(
      save_to,
      s"${save_to}_date_language"
    )
  }

  private def runJdbcCreateIndex(conn: Conn)(implicit spark: SparkSession): Unit = {
    log.info("runJdbcCreateIndex...")
    MyJdbcUtils(conn).createIndex(
      save_to,
      s"${save_to}_date_index",
      Seq("date")
    )
  }

  private def runJdbcDropIndex(conn: Conn)(implicit spark: SparkSession): Unit = {
    log.info("runJdbcDropIndex...")
    MyJdbcUtils(conn).dropIndex(
      save_to,
      s"${save_to}_date_language_index"
    )
  }

  private def runJdbcDatasetUpsert(conn: Conn)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    log.info("runJdbcDatasetUpsert...")

    val schema = new StructType()
      .add("date", DateType)
      .add("language", StringType)
      .add("users_count", IntegerType)

    val data = Seq(
      ("2010-01-23", "Java", 21000),
      ("2010-01-23", "Python", 100000),
      ("2010-01-23", "Scala", 15000),
      ("2015-08-15", "Java", 24000),
      ("2015-08-15", "Python", 135000),
      ("2015-08-15", "Scala", 2200),
      ("2015-08-15", "Rust", 1000)
    )
    val df = spark
      .createDataFrame(data)
      .toDF("date", "language", "users_count")
      .withColumn("date", to_date($"date", "yyyy-MM-dd"))

    MyJdbcUtils(conn).upsertTable(
      df,
      save_to,
      None,
      false,
      Seq("date", "language"),
      "doUpdate"
    )
  }

  private def runJdbcDatasetDelete(conn: Conn)(implicit spark: SparkSession): Unit = {
    log.info("runJdbcDatasetDelete...")

    val del = "date = '2010-01-23' and language = 'Scala'"
    MyJdbcUtils(conn).deleteByConditions(save_to, del)
  }

  private def runJdbcDropTable(conn: Conn)(implicit spark: SparkSession): Unit = {
    log.info("runJdbcDropTable...")
    MyJdbcUtils(conn).dropTable(save_to)
  }
}
