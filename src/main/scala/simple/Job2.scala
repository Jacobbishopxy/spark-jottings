package jotting.simple

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

object Job2 {

  val configFile = "application.conf"

  case class Conn(
      db: String,
      driver: String,
      host: String,
      port: Int,
      database: String,
      user: String,
      password: String
  ) {
    def url: String = s"jdbc:$db://$host:$port/$database?zeroDateTimeBehavior=convertToNull"

    def options: Map[String, String] = Map(
      "url"      -> this.url,
      "driver"   -> driver,
      "user"     -> user,
      "password" -> password
    )
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val config: Config = ConfigFactory.load(configFile).getConfig("job2")
    val conn = Conn(
      config.getString("db"),
      config.getString("driver"),
      config.getString("host"),
      config.getInt("port"),
      config.getString("database"),
      config.getString("user"),
      config.getString("password")
    )

    runJdbcDatasetWrite(conn)

    spark.stop()
  }

  private def createDataFrame(spark: SparkSession): Unit = {
    import spark.implicits._

    val columns = Seq("language", "users_count")
    val data    = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    // 1. Create DF from RDD
    val rdd = spark.sparkContext.parallelize(data)

    // 1.1 `toDF()` function
    val dfFromRDD1 = rdd.toDF()
    dfFromRDD1.printSchema()

    val dfFromRDD1WithColumnName = rdd.toDF("language", "users_count")
    dfFromRDD1WithColumnName.printSchema()

    // 1.2 `createDataFrame()` from SparkSession
    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns: _*)

    // 1.3 `createDataFrame()` with the Row type
    val schema = StructType(
      Array(
        StructField("language", StringType, true),
        StructField("users", StringType, true)
      )
    )
    val rowRDD     = rdd.map(attributes => Row(attributes._1, attributes._2))
    val dfFromRDD3 = spark.createDataFrame(rowRDD, schema)

    // 2. Create DF from List & Seq
    // 2.1 `toDF` make sure importing `import spark.implicits._`
    val dfFromData1 = data.toDF()

    // 2.2 `createDataFrame()` from SparkSession
    val dfFromData2 = spark.createDataFrame(data).toDF(columns: _*)

    // 2.3 `createDataFrame()` with the Row type
    import scala.collection.JavaConversions._
    val rowData     = Seq(Row("Java", "20000"), Row("Python", "100000"), Row("Scala", "3000"))
    val dfFromData3 = spark.createDataFrame(rowData, schema)
  }

  private def runJdbcDatasetWrite(conn: Conn)(implicit spark: SparkSession): Unit = {
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val df   = spark.createDataFrame(data).toDF("language", "users_count")

    println("============================================================================")
    println("start writing")

    // mode:
    // overwrite
    // append
    // ignore
    // error
    df.write.format("jdbc").options(conn.options).option("dbtable", "dev").mode("overwrite").save()

    println("end writing")
    println("============================================================================")
  }

  private def runJdbcDatasetRead(conn: Conn)(implicit spark: SparkSession): Unit = {
    println("============================================================================")
    println("start reading")
    val df =
      spark.read.format("jdbc").options(conn.options).option("dbtable", "SELECT * FROM dev").load()

    println(df)

    println("============================================================================")
    println("end reading")
  }
}
