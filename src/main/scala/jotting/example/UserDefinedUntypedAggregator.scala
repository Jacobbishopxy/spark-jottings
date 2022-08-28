package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

object UserDefinedUntypedAggregator {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark untyped aggregator example")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.json("src/main/resources/employees.json")
    df.createOrReplaceTempView("employees")
    println("input data:")
    df.show()

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()

    spark.stop()
  }

  case class Average(var sum: Long, var count: Long)

  object MyAverage extends Aggregator[Long, Average, Double] {
    def zero: Average = Average(0, 0)
    def reduce(buffer: Average, data: Long): Average = {
      buffer.sum += data
      buffer.count += 1
      buffer
    }
    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    def bufferEncoder: Encoder[Average] = Encoders.product
    def outputEncoder: Encoder[Double]  = Encoders.scalaDouble
  }
}
