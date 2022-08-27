package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

// 可以使用 case class 定义一个 RDD 的 schema
case class Record(key: Int, value: String)

object RDDRelation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  private def process(spark: SparkSession): Unit = {

    import spark.implicits._

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))

    // 任何包含 case classes 的 RDD 皆可用于创建临时视图。视图的 schema 是由 Scala 反射自动推导的。
    df.createOrReplaceTempView("records")

    // 一旦表被注册，则可以使用 SQL 进行查询
    println("Result of SELECT*:")
    spark.sql("SELECT * FROM records").collect().foreach(println)

    // 聚合查询同样也是支持的
    val count = spark.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // SQL 查询的结果为 RDDs 且支持所有通常的 RDD 函数。RDD 中项的类型为 Row，它允许用户顺序的访问每个列。
    val rddFromSql = spark.sql("SELECT key, value FROM records WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // 查询同样也可以使用 LINQ 类似的 Scala DSL
    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)

    // 使用 overwrite 模式将 RDD 写入 parquet
    df.write.mode(SaveMode.Overwrite).parquet("pair.parquet")

    // 读取 parquet 文件，其本身为自描述的，因此 schema 是保留的。
    val parquetFile = spark.read.parquet("pair.parquet")

    // 可以在 parquet 文件上使用 DSL 进行查询，正如普通的 RDD 那样。
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)

    parquetFile.createOrReplaceTempView("parquetFile")
    spark.sql("SELECT * FROM parquetFile").collect().foreach(println)
  }

}
