package jotting.example

import java.util.Properties

import org.apache.spark.sql.SparkSession

/*
As an example, we omitted real files in this project. If test is required, please download
real files from [Spark project](https://github.com/apache/spark).
 */
object DataSources {
  val resources = "src/main/resources/"

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    runGenericFileSourceOptionsExample(spark)
    runBasicDataSourceExample(spark)
    runBasicParquetExample(spark)
    runParquetSchemaMergingExample(spark)
    runJsonDatasetExample(spark)
    runCsvDatasetExample(spark)
    runTextDatasetExample(spark)
    runJdbcDatasetExample(spark)

    spark.stop()
  }

  private def runGenericFileSourceOptionsExample(spark: SparkSession): Unit = {
    // 通过数据源选项开启忽略已损坏的文件
    val testCorruptDF0 = spark.read
      .option("ignoreCorruptFiles", "true")
      .parquet(
        resources + "dir1/",
        resources + "dir1/dir2/"
      )
    testCorruptDF0.show()

    // 通过配置开启忽略已损坏的文件
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
    val testCorruptDF1 = spark.read.parquet(
      resources + "dir1/",
      resources + "dir1/dir2/"
    )
    testCorruptDF1.show()

    // 数据源选项，递归查询
    val recursiveLoadedDF = spark.read
      .format("parquet")
      .option("recursiveFileLookup", "true")
      .load(resources + "dir1")
    recursiveLoadedDF.show()

    // 筛选 parquet 文件
    spark.sql("set spark.sql.files.ignoreCorruptFiles=false")
    val testGlobFilterDF = spark.read
      .format("parquet")
      .option("pathGlobFilter", "*.parquet")
      .load(resources + "dir1")
    testGlobFilterDF.show()

    // 筛选日期前修改的文件
    val beforeFilterDF = spark.read
      .format("parquet")
      .option("modifiedBefore", "2020-07-01T05:30:00")
      .load(resources + "dir1")
    beforeFilterDF.show()

    // 筛选日期修改后的文件
    val afterFilterDF = spark.read
      .format("parquet")
      .option("modifiedAfter", "2020-06-01T05:30:00")
      .load(resources + "dir1")
    afterFilterDF.show()
  }

  private def runBasicDataSourceExample(spark: SparkSession): Unit = {
    val usersDF = spark.read.load(resources + "users.parquet")
    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

    val peopleDF = spark.read.format("json").load(resources + "people.json")
    peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

    val peopleDFCsv = spark.read
      .format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(resources + "people.csv")

    usersDF.write
      .format("orc")
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .option("orc.column.encoding.direct", "name")
      .save("users_with_options.orc")

    usersDF.write
      .format("parquet")
      .option("parquet.bloom.filter.enabled#favorite_color", "true")
      .option("parquet.bloom.filter.expected.ndv#favorite_color", "1000000")
      .option("parquet.enable.dictionary", "true")
      .option("parquet.page.write-checksum.enabled", "false")
      .save("users_with_options.parquet")

    val sqlDF = spark.sql(s"SELECT * FROM parquet.`$resources/users.parquet`")

    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")

    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

    usersDF.write
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("users_partitioned_bucketed")

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")
  }

  private def runBasicParquetExample(spark: SparkSession): Unit = {
    // 引入普通类型的 Encoders
    import spark.implicits._

    val peopleDF = spark.read.json(resources + "people.json")

    // 写入 parquet
    peopleDF.write.parquet("people.parquet")

    val parquetFileDF = spark.read.parquet("people.parquet")

    // parquet 文件同样也可以用作于创建临时视图并使用 SQL 语句
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
  }

  private def runParquetSchemaMergingExample(spark: SparkSession): Unit = {
    import spark.implicits._

    // 创建一个简单的 DataFrame 并存储至分区路径
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.parquet("data/test_table/key=1")

    // 创建另一个 DataFrame 并存储至分区路径
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.parquet("data/test_table/key=2")

    // 读取分区表
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()
  }

  private def runJsonDatasetExample(spark: SparkSession): Unit = {
    import spark.implicits._

    // 读取 json
    val path     = resources + "people.json"
    val peopleDF = spark.read.json(path)

    peopleDF.printSchema()

    // 创建临时视图
    peopleDF.createOrReplaceTempView("people")

    // 可执行 SQL 语句
    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()

    // 或者，DataFrame 可以有 JSON dataset 所创建，其表现为一个存储若干 JSON 对象字符串的 Dataset[String]
    val otherPeopleDataset = spark.createDataset(
      """
      {"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}
      """ :: Nil
    )
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()
  }

  private def runCsvDatasetExample(spark: SparkSession): Unit = {
    val path = resources + "people.csv"

    val df = spark.read.csv(path)
    df.show()

    val df2 = spark.read.option("delimiter", ";").csv(path)
    df2.show()

    val df3 = spark.read.option("delimiter", ";").option("header", "true").csv(path)
    df3.show()

    // 可以通过 options() 使用若干配置
    val df4 = spark.read.options(Map("delimiter" -> ";", "header" -> "true")).csv(path)
    // ”output“ 是文件夹，其包含若干 CSV 文件以及一个 _SUCCESS 文件
    df4.write.csv("output")

    // 读取文件夹中的所有文件，请确保只能有 CSV 在文件夹中
    val folderPath = resources
    val df5        = spark.read.csv(folderPath)
    // 错误的 schema 因为非 CSV 文件被读取了
    df5.show()
  }

  private def runTextDatasetExample(spark: SparkSession): Unit = {
    val path = resources + "people.txt"

    val df1 = spark.read.text(path)
    df1.show()

    val df2 = spark.read.option("lineSep", ",").text(path)
    df2.show()

    val df3 = spark.read.option("wholetext", true).text(path)
    df3.show()

    // "ouput" 是文件夹，其包含若干 text 文件以及一个 _SUCCESS 文件
    df1.write.text("output")

    // 可以通过 'compression' 选项指定压缩格式
    df1.write.option("compression", "gzip").text("output_compressed")
  }

  /*
  spark-submit --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
   */
  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
    // 注意：JDBC 的加载与保存是可以既通过 load/save 亦或是 JDBC 配置方法

    // 通过 JDBC 源加载数据
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()

    // 通过 JDBC 配置加载数据
    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")
    val jdbcDF2 =
      spark.read.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // 为读取 schema 指定自定义数据类型
    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
    val jdbcDF3 =
      spark.read.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // 通过 JDBC 源保存数据
    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()

    // 通过 JDBC 配置保存数据
    jdbcDF2.write.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // 为保存时创建表指定数据结构
    jdbcDF3.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
  }
}
