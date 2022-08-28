package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object DFPivotAndUnpivot {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame pivot and unpivot example")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  val data = Seq(
    ("Banana", 1000, "USA"),
    ("Carrots", 1500, "USA"),
    ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"),
    ("Orange", 2000, "USA"),
    ("Banana", 400, "China"),
    ("Carrots", 1200, "China"),
    ("Beans", 1500, "China"),
    ("Orange", 4000, "China"),
    ("Banana", 2000, "Canada"),
    ("Carrots", 2000, "Canada"),
    ("Beans", 2000, "Mexico")
  )

  private def process(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = data.toDF("Product", "Amount", "Country")
    df.show()

    // 1. pivot
    val countries = Seq("USA", "China", "Canada", "Mexico")
    val pivotDF   = df.groupBy("Product").pivot("Country", countries).sum("Amount")
    pivotDF.show()
    /*
    +-------+------+-----+------+----+
    |Product|Canada|China|Mexico| USA|
    +-------+------+-----+------+----+
    | Orange|  null| 4000|  null|4000|
    |  Beans|  null| 1500|  2000|1600|
    | Banana|  2000|  400|  null|1000|
    |Carrots|  2000| 1200|  null|1500|
     */

    //  2. unpivot
    val expr1 = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
    val unPivotDF = pivotDF.select($"Product", expr(expr1)).where("Total is not null")
    /*
    +-------+-------+-----+
    |Product|Country|Total|
    +-------+-------+-----+
    | Orange|  China| 4000|
    |  Beans|  China| 1500|
    |  Beans| Mexico| 2000|
    | Banana| Canada| 2000|
    | Banana|  China|  400|
    |Carrots| Canada| 2000|
    |Carrots|  China| 1200|
    +-------+-------+-----+
     */
  }
}
