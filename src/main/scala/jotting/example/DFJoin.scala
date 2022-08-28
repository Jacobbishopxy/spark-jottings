package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DFJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame join example")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  /*
  `join()` 方法与语法：

  1) join(right: Dataset[_]): DataFrame
  2) join(right: Dataset[_], usingColumn: String): DataFrame
  3) join(right: Dataset[_], usingColumns: Seq[String]): DataFrame
  4) join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame
  5) join(right: Dataset[_], joinExprs: Column): DataFrame
  6) join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame
   */
  private def process(spark: SparkSession): Unit = {
    import spark.implicits._

    val empDF = emp.toDF(empColumns: _*)
    empDF.show(false)
    /*
    Emp Dataset
    +------+--------+---------------+-----------+-----------+------+------+
    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|
    +------+--------+---------------+-----------+-----------+------+------+
    |1     |Smith   |-1             |2018       |10         |M     |3000  |
    |2     |Rose    |1              |2010       |20         |M     |4000  |
    |3     |Williams|1              |2010       |10         |M     |1000  |
    |4     |Jones   |2              |2005       |10         |F     |2000  |
    |5     |Brown   |2              |2010       |40         |      |-1    |
    |6     |Brown   |2              |2010       |50         |      |-1    |
    +------+--------+---------------+-----------+-----------+------+------+
     */

    val deptDF = dept.toDF(deptColumns: _*)
    deptDF.show(false)
    /*
    Dept Dataset
    +---------+-------+
    |dept_name|dept_id|
    +---------+-------+
    |Finance  |10     |
    |Marketing|20     |
    |Sales    |30     |
    |IT       |40     |
    +---------+-------+
     */

    // 1. Inner join
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner")
      .show(false)
    /*
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
     */

    // 2. Full outer join
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "outer")
      .show(false)
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full")
      .show(false)
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter")
      .show(false)
    /*
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    |6     |Brown   |2              |2010       |50         |      |-1    |null     |null   |
    |null  |null    |null           |null       |null       |null  |null  |Sales    |30     |
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
     */

    // 3. Left outer join
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left")
      .show(false)
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftouter")
      .show(false)
    /*
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    |6     |Brown   |2              |2010       |50         |      |-1    |null     |null   |
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
     */

    // 4. Right outer join
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "right")
      .show(false)
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "rightouter")
      .show(false)
    /*
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    |null  |null    |null           |null       |null       |null  |null  |Sales    |30     |
    |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
     */

    // 5. Left semi join
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftsemi")
      .show(false)
    /*
    leftsemi join
    +------+--------+---------------+-----------+-----------+------+------+
    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|
    +------+--------+---------------+-----------+-----------+------+------+
    |1     |Smith   |-1             |2018       |10         |M     |3000  |
    |2     |Rose    |1              |2010       |20         |M     |4000  |
    |3     |Williams|1              |2010       |10         |M     |1000  |
    |4     |Jones   |2              |2005       |10         |F     |2000  |
    |5     |Brown   |2              |2010       |40         |      |-1    |
    +------+--------+---------------+-----------+-----------+------+------+
     */

    // 6. Left anti join
    empDF
      .join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftanti")
      .show(false)
    /*
    +------+-----+---------------+-----------+-----------+------+------+
    |emp_id|name |superior_emp_id|year_joined|emp_dept_id|gender|salary|
    +------+-----+---------------+-----------+-----------+------+------+
    |6     |Brown|2              |2010       |50         |      |-1    |
    +------+-----+---------------+-----------+-----------+------+------+
     */

    // 7. Self join
    empDF
      .as("emp1")
      .join(empDF.as("emp2"), col("emp1.superior_emp_id") === col("emp2.emp_id"), "inner")
      .select(
        col("emp1.emp_id"),
        col("emp1.name"),
        col("emp2.emp_id").as("superior_emp_id"),
        col("emp2.name").as("superior_emp_name")
      )
      .show(false)
    /*
    +------+--------+---------------+-----------------+
    |emp_id|name    |superior_emp_id|superior_emp_name|
    +------+--------+---------------+-----------------+
    |2     |Rose    |1              |Smith            |
    |3     |Williams|1              |Smith            |
    |4     |Jones   |2              |Rose             |
    |5     |Brown   |2              |Rose             |
    |6     |Brown   |2              |Rose             |
    +------+--------+---------------+-----------------+
     */

    // 8. SQL 表达式
    empDF.createOrReplaceTempView("EMP")
    deptDF.createOrReplaceTempView("DEPT")

    // SQL JOIN
    val joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")
    joinDF.show(false)

    val joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")
    joinDF2.show(false)

  }

  val emp = Seq(
    (1, "Smith", -1, "2018", "10", "M", 3000),
    (2, "Rose", 1, "2010", "20", "M", 4000),
    (3, "Williams", 1, "2010", "10", "M", 1000),
    (4, "Jones", 2, "2005", "10", "F", 2000),
    (5, "Brown", 2, "2010", "40", "", -1),
    (6, "Brown", 2, "2010", "50", "", -1)
  )
  val empColumns =
    Seq("emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary")

  val dept = Seq(
    ("Finance", 10),
    ("Marketing", 20),
    ("Sales", 30),
    ("IT", 40)
  )
  val deptColumns = Seq("dept_name", "dept_id")

}
