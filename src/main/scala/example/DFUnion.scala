package jotting.example

import org.apache.spark.sql.SparkSession

object DFUnion {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame union example")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  val simpleData = Seq(
    ("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "NY", 86000, 56, 20000),
    ("Robert", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000)
  )

  val simpleData2 = Seq(
    ("James", "Sales", "NY", 90000, 34, 10000),
    ("Maria", "Finance", "CA", 90000, 24, 23000),
    ("Jen", "Finance", "NY", 79000, 53, 15000),
    ("Jeff", "Marketing", "CA", 80000, 25, 18000),
    ("Kumar", "Marketing", "NY", 91000, 50, 21000)
  )

  private def process(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    df.printSchema()
    df.show()
    /*
    root
    |-- employee_name: string (nullable = true)
    |-- department: string (nullable = true)
    |-- state: string (nullable = true)
    |-- salary: integer (nullable = false)
    |-- age: integer (nullable = false)
    |-- bonus: integer (nullable = false)

    +-------------+----------+-----+------+---+-----+
    |employee_name|department|state|salary|age|bonus|
    +-------------+----------+-----+------+---+-----+
    |        James|     Sales|   NY| 90000| 34|10000|
    |      Michael|     Sales|   NY| 86000| 56|20000|
    |       Robert|     Sales|   CA| 81000| 30|23000|
    |        Maria|   Finance|   CA| 90000| 24|23000|
    +-------------+----------+-----+------+---+-----+
     */

    val df2 = simpleData2.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    /*
    +-------------+----------+-----+------+---+-----+
    |employee_name|department|state|salary|age|bonus|
    +-------------+----------+-----+------+---+-----+
    |James        |Sales     |NY   |90000 |34 |10000|
    |Maria        |Finance   |CA   |90000 |24 |23000|
    |Jen          |Finance   |NY   |79000 |53 |15000|
    |Jeff         |Marketing |CA   |80000 |25 |18000|
    |Kumar        |Marketing |NY   |91000 |50 |21000|
    +-------------+----------+-----+------+---+-----+
     */

    // 组合两个或以上的 DF
    val df3 = df.union(df2)
    df3.show(false)

    val df4 = df.unionAll(df2)
    df4.show(false)

    // 组合，无重复
    val df5 = df.union(df2).distinct()
    df5.show(false)

  }
}
