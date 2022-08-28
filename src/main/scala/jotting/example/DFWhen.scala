package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{when, col, expr}

object DFWhen {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame when example")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  val data = List(
    ("James", "", "Smith", "36636", "M", 60000),
    ("Michael", "Rose", "", "40288", "M", 70000),
    ("Robert", "", "Williams", "42114", "", 400000),
    ("Maria", "Anne", "Jones", "39192", "F", 500000),
    ("Jen", "Mary", "Brown", "", "F", 0)
  )

  val cols = Seq("first_name", "middle_name", "last_name", "dob", "gender", "salary")

  private def process(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.createDataFrame(data).toDF(cols: _*)

    // 1. when otherwise
    val df2 = df
      .withColumn(
        "new_gender",
        when(col("gender") === "M", "Mail")
          .when(col("gender") === "F", "Female")
          .otherwise("Unknown")
      )

    // 同样也可用作于 SQL 的 select 声明
    val df3 = df.select(
      col("*"),
      when(col("gender") === "M", "Male")
        .when(col("gender") === "F", "Female")
        .otherwise("Unknown")
        .alias("new_gender")
    )

    // 2. case when
    val expr1 = """
    case 
      when gender = 'M' then 'Male'
      when gender = 'F' then 'Female'
      else 'Unknown'
    end
    """

    val df4 = df.withColumn("new_gender", expr(expr1).alias("new_gender"))

    // SQL select
    val df5 = df.select(col("*"), expr(expr1).alias("new_gender"))

    // 3. && 与 || 操作符
    val dataDF = Seq(
      (66, "a", "4"),
      (67, "a", "0"),
      (70, "b", "4"),
      (71, "d", "4")
    ).toDF("id", "code", "amt")

    val whenCase = when(col("code") === "a" || col("code") === "d", "A")
      .when(col("code") === "b" && col("amt") === "4", "B")
      .otherwise("A1")
    dataDF.withColumn("new_column", whenCase).show()

  }
}
