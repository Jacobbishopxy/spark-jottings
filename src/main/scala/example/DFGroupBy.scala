package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DFGroupBy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame groupBy example")
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

  /*
  DF 调用 `groupBy()` 后返回 `RelationalGroupedDataset` 对象，其包含了一下聚合函数：
  - `count()`
  - `mean()`
  - `max()`
  - `min()`
  - `sum()`
  - `avg()`
  - `agg()`: 该方法允许同一时间内进行多个聚合计算
  - `pivot()`
   */
  private def process(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    df.show()
    /*
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

    // 对 DF 列进行 groupBy 与聚合
    df.groupBy("department").sum("salary").show(false)
    df.groupBy("department").count()
    df.groupBy("department").min("salary")
    df.groupBy("department").max("salary")
    df.groupBy("department").avg("salary")
    df.groupBy("department").mean("salary")
    /*
    +----------+-----------+
    |department|sum(salary)|
    +----------+-----------+
    |Sales     |257000     |
    |Finance   |351000     |
    |Marketing |171000     |
    +----------+-----------+
     */

    // 对多列进行 groupBy 与聚合
    df.groupBy("department", "state")
      .sum("salary", "bonus")
      .show()
    /*
    +----------+-----+-----------+----------+
    |department|state|sum(salary)|sum(bonus)|
    +----------+-----+-----------+----------+
    |Finance   |NY   |162000     |34000     |
    |Marketing |NY   |91000      |21000     |
    |Sales     |CA   |81000      |23000     |
    |Marketing |CA   |80000      |18000     |
    |Finance   |CA   |189000     |47000     |
    |Sales     |NY   |176000     |30000     |
    +----------+-----+-----------+----------+
     */

    // 使用 `agg()` 聚合函数执行多个聚合计算
    df.groupBy("department")
      .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus")
      )
      .show(false)
    /*
    +----------+----------+-----------------+---------+---------+
    |department|sum_salary|avg_salary       |sum_bonus|max_bonus|
    +----------+----------+-----------------+---------+---------+
    |Sales     |257000    |85666.66666666667|53000    |23000    |
    |Finance   |351000    |87750.0          |81000    |24000    |
    |Marketing |171000    |85500.0          |39000    |21000    |
    +----------+----------+-----------------+---------+---------+
     */

    // 对聚合数据进行筛选

    df.groupBy("department")
      .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus")
      )
      .where(col("sum_bonus") >= 50000)
      .show(false)
    /*
    +----------+----------+-----------------+---------+---------+
    |department|sum_salary|avg_salary       |sum_bonus|max_bonus|
    +----------+----------+-----------------+---------+---------+
    |Sales     |257000    |85666.66666666667|53000    |23000    |
    |Finance   |351000    |87750.0          |81000    |24000    |
    +----------+----------+-----------------+---------+---------+
     */
  }
}
