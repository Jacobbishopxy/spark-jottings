package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object SqlUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL UDF example")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  val columns = Seq("Seqno", "Quote")
  val data = Seq(
    ("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy.")
  )

  private def process(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = data.toDF(columns: _*)
    df.show()
    /*
    +-----+-----------------------------------------------------------------------------+
    |Seqno|Quote                                                                        |
    +-----+-----------------------------------------------------------------------------+
    |1    |Be the change that you wish to see in the world                              |
    |2    |Everyone thinks of changing the world, but no one thinks of changing himself.|
    |3    |The purpose of our lives is to be happy.                                     |
    +-----+-----------------------------------------------------------------------------+
     */

    // 创建函数
    val convertCase = (strQuote: String) => {
      val arr = strQuote.split(" ")
      arr.map(f => f.substring(0, 1).toUpperCase() + f.substring(1, f.length)).mkString(" ")
    }
    // 包裹 udf
    val convertUDF = udf(convertCase)
    // 使用 udf
    df.select($"Seqno", convertUDF($"Quote").as("Quote")).show(false)
    /*
    +-----+-----------------------------------------------------------------------------+
    |Seqno|Quote                                                                        |
    +-----+-----------------------------------------------------------------------------+
    |1    |Be The Change That You Wish To See In The World                              |
    |2    |Everyone Thinks Of Changing The World, But No One Thinks Of Changing Himself.|
    |3    |The Purpose Of Our Lives Is To Be Happy.                                     |
    +-----+-----------------------------------------------------------------------------+
     */

    // 注册 UDF 并在 SQL 中使用
    spark.udf.register("convertUDF", convertCase)
    df.createOrReplaceTempView("QUOTE_TABLE")
    spark.sql("SELECT Seqno, convertUDF(Quote) FROM QUOTE_TABLE")
  }
}
