package jotting.example

import org.apache.spark.sql.{Row, SparkSession, DataFrame}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions.{lit, col}

/*
`withColumn()` 函数是 DataFrame 的一个转换函数，用作于操作 DF 所有行或者所选行的列数值。
在执行添加新列，更新已有列数值，从已有列衍生新列等操作后，返回一个新的 DF。
 */
object DFWithColumn {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame withColumn example")
      .getOrCreate()

    import spark.implicits._

    process(spark)
    process2(spark)

    spark.stop()
  }

  private val data = Seq(
    Row(Row("James;", "", "Smith"), "36636", "M", "3000"),
    Row(Row("Michael", "Rose", ""), "40288", "M", "4000"),
    Row(Row("Robert", "", "Williams"), "42114", "M", "4000"),
    Row(Row("Maria", "Anne", "Jones"), "39192", "F", "4000"),
    Row(Row("Jen", "Mary", "Brown"), "", "F", "-1")
  )

  private val schema = new StructType()
    .add(
      "name",
      new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType)
    )
    .add("dob", StringType)
    .add("gender", StringType)
    .add("salary", StringType)

  private def process(spark: SparkSession): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // 1. DF 新增列，可链式操作多列
    df.withColumn("Country", lit("USA")).withColumn("anotherColumn", lit("anotherValue"))

    // 2. 修改已有列的值
    df.withColumn("salary", col("salary") * 100)

    // 3. 从已有列中衍生新列
    df.withColumn("CopiedColumn", col("salary") * -1)

    // 4. 修改列类型
    df.withColumn("salary", col("salary").cast("Integer"))

    // 5. 新增、替换、或更新多列
    // 当用户想要对 DF 新增、替换、或更新多列时，不建议链式调用 `withColumn()` 函数，
    // 因为这会影响性能。因此更为推荐的做法是为 DF 创建一个临时视图后，使用 `select()`。
    df.createOrReplaceTempView("PERSON")
    spark
      .sql("SELECT salary*100 as salary, salary*-1 as CopiedColumn, 'USA' as country FROM PERSON")
      .show()

    // 6. 重命名列名
    df.withColumnRenamed("gender", "sex")

    // 7. 删除列
    df.drop("CopiedColumn")
  }

  // 8. 拆分列成为若干列
  // 本例虽未用到 `withColumn()`，但通过 `map()` 转换函数做到了拆分 DF 列成为若干列。
  private def process2(spark: SparkSession): Unit = {
    import spark.implicits._

    val columns = Seq("name", "address")
    val data = Seq(
      ("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
      ("Maria, Garcia", "3456 Walnut st, Newark, NJ, 94732")
    )
    val dfFromData = spark.createDataFrame(data).toDF(columns: _*)
    dfFromData.printSchema()

    val newDF = dfFromData.map(f => {
      val nameSplit = f.getAs[String](0).split(",")
      val addSplit  = f.getAs[String](1).split(",")
      (
        nameSplit(0),
        nameSplit(1),
        addSplit(0),
        addSplit(1),
        addSplit(2),
        addSplit(3)
      )
    })
    val finalDF = newDF.toDF("First Name", "Last Name", "Address Line1", "City", "State", "zipCode")
    finalDF.printSchema()
    finalDF.show(false)
  }

}
