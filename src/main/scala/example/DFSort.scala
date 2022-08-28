package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DFSort {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame sort example")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  val simpleData = Seq(
    ("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "NY", 86000, 56, 20000),
    ("Robert", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000),
    ("Raman", "Finance", "CA", 99000, 40, 24000),
    ("Scott", "Finance", "NY", 83000, 36, 19000),
    ("Jen", "Finance", "NY", 79000, 53, 15000),
    ("Jeff", "Marketing", "CA", 80000, 25, 18000),
    ("Kumar", "Marketing", "NY", 91000, 50, 21000)
  )

  private def process(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
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
    |        Raman|   Finance|   CA| 99000| 40|24000|
    |        Scott|   Finance|   NY| 83000| 36|19000|
    |          Jen|   Finance|   NY| 79000| 53|15000|
    |         Jeff| Marketing|   CA| 80000| 25|18000|
    |        Kumar| Marketing|   NY| 91000| 50|21000|
    +-------------+----------+-----+------+---+-----+
     */

    // `sort()` 函数
    df.sort("department", "state").show(false)
    df.sort(col("department"), col("state")).show(false)
    /*
    +-------------+----------+-----+------+---+-----+
    |employee_name|department|state|salary|age|bonus|
    +-------------+----------+-----+------+---+-----+
    |Maria        |Finance   |CA   |90000 |24 |23000|
    |Raman        |Finance   |CA   |99000 |40 |24000|
    |Jen          |Finance   |NY   |79000 |53 |15000|
    |Scott        |Finance   |NY   |83000 |36 |19000|
    |Jeff         |Marketing |CA   |80000 |25 |18000|
    |Kumar        |Marketing |NY   |91000 |50 |21000|
    |Robert       |Sales     |CA   |81000 |30 |23000|
    |James        |Sales     |NY   |90000 |34 |10000|
    |Michael      |Sales     |NY   |86000 |56 |20000|
    +-------------+----------+-----+------+---+-----+
     */

    // `orderBy()` 函数
    df.orderBy("department", "state").show(false)
    df.orderBy(col("department"), col("state")).show(false)

    // ASC 排序
    df.sort(col("department").asc, col("state").asc).show(false)
    df.orderBy(col("department").asc, col("state").asc).show(false)
    /*
    +-------------+----------+-----+------+---+-----+
    |employee_name|department|state|salary|age|bonus|
    +-------------+----------+-----+------+---+-----+
    |Maria        |Finance   |CA   |90000 |24 |23000|
    |Raman        |Finance   |CA   |99000 |40 |24000|
    |Jen          |Finance   |NY   |79000 |53 |15000|
    |Scott        |Finance   |NY   |83000 |36 |19000|
    |Jeff         |Marketing |CA   |80000 |25 |18000|
    |Kumar        |Marketing |NY   |91000 |50 |21000|
    |Robert       |Sales     |CA   |81000 |30 |23000|
    |James        |Sales     |NY   |90000 |34 |10000|
    |Michael      |Sales     |NY   |86000 |56 |20000|
    +-------------+----------+-----+------+---+-----+
     */

    // DESC 排序
    df.sort(col("department").asc, col("state").desc).show(false)
    df.orderBy(col("department").asc, col("state").desc).show(false)
    /*
    +-------------+----------+-----+------+---+-----+
    |employee_name|department|state|salary|age|bonus|
    +-------------+----------+-----+------+---+-----+
    |Scott        |Finance   |NY   |83000 |36 |19000|
    |Jen          |Finance   |NY   |79000 |53 |15000|
    |Raman        |Finance   |CA   |99000 |40 |24000|
    |Maria        |Finance   |CA   |90000 |24 |23000|
    |Kumar        |Marketing |NY   |91000 |50 |21000|
    |Jeff         |Marketing |CA   |80000 |25 |18000|
    |James        |Sales     |NY   |90000 |34 |10000|
    |Michael      |Sales     |NY   |86000 |56 |20000|
    |Robert       |Sales     |CA   |81000 |30 |23000|
    +-------------+----------+-----+------+---+-----+
     */

    // 使用 Sorting 函数与 `select`
    df.select($"employee_name", asc("department"), desc("state"), $"salary", $"age", $"bonus")
      .show(false)

    // 在 SQL 上使用 Sorting
    df.createOrReplaceTempView("EMP")
    spark
      .sql("SELECT employee_name, asc('department'), desc('state'), salary, age, bonus FROM EMP")
      .show(false)

  }
}
