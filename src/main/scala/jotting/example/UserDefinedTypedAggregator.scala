package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

object UserDefinedTypedAggregator {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark typed aggregator example")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.read.json("src/main/resources/employees.json")
    println("input data:")
    ds.show()

    val averageSalary = MyAverage.toColumn.name("average_salary")
    val result        = ds.select(averageSalary)
    result.show()

    spark.stop()
  }

  case class Employee(name: String, salary: Long)
  case class Average(var sum: Long, var count: Long)

  object MyAverage extends Aggregator[Employee, Average, Double] {
    def zero: Average = Average(0, 0)
    def reduce(buffer: Average, employee: Employee): Average = {
      buffer.sum += employee.salary
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
