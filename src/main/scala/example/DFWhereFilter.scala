package jotting.example

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{ArrayType, StructType, StringType}
import org.apache.spark.sql.functions.{col, array_contains}

object DFWhereFilter {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame filter and where example")
      .getOrCreate()

    import spark.implicits._

    spark.stop()
  }

  val arrayStructureData = Seq(
    Row(Row("James", "", "Smith"), List("Java", "Scala", "C++"), "OH", "M"),
    Row(Row("Anna", "Rose", ""), List("Spark", "Java", "C++"), "NY", "F"),
    Row(Row("Julia", "", "Williams"), List("CSharp", "VB"), "OH", "F"),
    Row(Row("Maria", "Anne", "Jones"), List("CSharp", "VB"), "NY", "M"),
    Row(Row("Jen", "Mary", "Brown"), List("CSharp", "VB"), "NY", "M"),
    Row(Row("Mike", "Mary", "Williams"), List("Python", "VB"), "OH", "M")
  )

  val arrayStructureSchema = new StructType()
    .add(
      "name",
      new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType)
    )
    .add("languages", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)

  /*
  `filter()` 语法：
  1) filter(condition: Column): Dataset[T]
  2) filter(conditionExpr: String): Dataset[T] //using SQL expression
  3) filter(func: T => Boolean): Dataset[T]
  4) filter(func: FilterFunction[T]): Dataset[T]
   */
  private def process(spark: SparkSession): Unit = {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),
      arrayStructureSchema
    )
    df.printSchema()
    df.show()

    import spark.implicits._

    // 1. condition
    df.filter(df("state") === "OH").show(false)

    // 以下皆可
    df.filter('state === "OH").show(false)
    df.filter($"state" === "OH").show(false)
    df.filter(col("state") === "OH").show(false)
    df.where('state === "OH").show(false)
    df.where($"state" === "OH").show(false)
    df.where(col("state") === "OH").show(false)

    // 2. conditionExpr
    df.filter("gender == 'M'").show(false)
    df.where("gender == 'M'").show(false)

    // 多条件的筛选
    df.filter(df("state") === "OH" && df("gender") === "M").show(false)

    // 数组列的筛选
    df.filter(array_contains(df("languages"), "Java")).show(false)

    // 嵌套结构列的筛选
    df.filter(df("name.lastname") === "Williams").show(false)
  }
}
