package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

object DFMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame union example")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  val structureData = Seq(
    Row("James", "", "Smith", "36636", "NewYork", 3100),
    Row("Michael", "Rose", "", "40288", "California", 4300),
    Row("Robert", "", "Williams", "42114", "Florida", 1400),
    Row("Maria", "Anne", "Jones", "39192", "Florida", 5500),
    Row("Jen", "Mary", "Brown", "34561", "NewYork", 3000)
  )

  val structureSchema = new StructType()
    .add("firstname", StringType)
    .add("middlename", StringType)
    .add("lastname", StringType)
    .add("id", StringType)
    .add("location", StringType)
    .add("salary", IntegerType)

  // Mock 类，用于模拟真实场景
  class Util extends Serializable {
    def combine(fname: String, mname: String, lname: String): String = {
      fname + "," + mname + "," + lname
    }
  }

  private def process(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)
    df.printSchema()
    df.show(false)
    /*
    root
    |-- firstname: string (nullable = true)
    |-- middlename: string (nullable = true)
    |-- lastname: string (nullable = true)
    |-- id: string (nullable = true)
    |-- location: string (nullable = true)
    |-- salary: integer (nullable = true)

    +---------+----------+--------+-----+----------+------+
    |firstname|middlename|lastname|id   |location  |salary|
    +---------+----------+--------+-----+----------+------+
    |James    |          |Smith   |36636|NewYork   |3100  |
    |Michael  |Rose      |        |40288|California|4300  |
    |Robert   |          |Williams|42114|Florida   |1400  |
    |Maria    |Anne      |Jones   |39192|Florida   |5500  |
    |Jen      |Mary      |Brown   |34561|NewYork   |3000  |
    +---------+----------+--------+-----+----------+------+
     */

    // `map()`
    val df2 = df.map(row => {
      // 初始化会发生在每一行数据，务必避免类似于数据库连接等消耗性能高的操作于此处
      val util     = new Util()
      val fullName = util.combine(row.getString(0), row.getString(1), row.getString(2))
      (fullName, row.getString(3), row.getInt(5))
    })
    val df2Map = df2.toDF("fullName", "id", "salary")

    df2Map.printSchema()
    df2Map.show(false)
    /*
    root
    |-- fullName: string (nullable = true)
    |-- id: string (nullable = true)
    |-- salary: integer (nullable = false)

    +----------------+-----+------+
    |fullName        |id   |salary|
    +----------------+-----+------+
    |James,,Smith    |36636|3100  |
    |Michael,Rose,   |40288|4300  |
    |Robert,,Williams|42114|1400  |
    |Maria,Anne,Jones|39192|5500  |
    |Jen,Mary,Brown  |34561|3000  |
    +----------------+-----+------+
     */

    // `mapPartitions()`
    val df3 = df.mapPartitions(iterator => {
      // 数据库连接等消耗性能高的操作于此处进行
      val util = new Util()
      val res = iterator.map(row => {
        val fullName = util.combine(row.getString(0), row.getString(1), row.getString(2))
        (fullName, row.getString(3), row.getInt(5))
      })
      res
    })
    val df3part = df3.toDF("fullName", "id", "salary")
    df3part.printSchema()
    df3part.show(false)
  }
}
