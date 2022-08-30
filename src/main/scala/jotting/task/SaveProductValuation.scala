package jotting.task

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object SaveProductValuation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Save product valuation, daily cron job")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  private def process(spark: SparkSession): Unit = {
    val queryStr = """
    SELECT *
    FROM ev_rpt_thirdvaluation
    WHERE
      tradeDate = (SELECT MAX(tradeDate) FROM ev_rpt_thirdvaluation)
    """
    val df = spark.read
      .format("jdbc")
      .options(Common.connCapRaw.options)
      .option("query", queryStr)
      .load()

    df.write
      .format("jdbc")
      .options(Common.connCapProd.options)
      .option("dbtable", "product_valuation")
      .mode("append")
      .save()
  }

}
