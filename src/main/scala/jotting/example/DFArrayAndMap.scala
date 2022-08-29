package jotting.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import scala.collection.mutable.LinkedHashSet

object DFArrayAndMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame ArrayType and MapType column example")
      .getOrCreate()

    process1(spark)
    process2(spark)

    spark.stop()
  }

  private def process1(spark: SparkSession): Unit = {
    // 创建 ArrayType
    val arrayCol         = DataTypes.createArrayType(StringType)
    val arrayColWithNull = DataTypes.createArrayType(StringType, true)
    // 通过 ArrayType 该 case class 创建
    val caseArrayCol = ArrayType(StringType, true)

    import spark.implicits._

    val arrayStructureData = Seq(
      Row("James,,Smith", List("Java", "Scala", "C++"), List("Spark", "Java"), "OH", "CA"),
      Row("Michael,Rose,", List("Spark", "Java", "C++"), List("Spark", "Java"), "NY", "NJ"),
      Row("Robert,,Williams", List("CSharp", "VB"), List("Spark", "Python"), "UT", "NV")
    )
    val arrayStructureSchema = new StructType()
      .add("name", StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("languagesAtWork", ArrayType(StringType))
      .add("currentState", StringType)
      .add("previousState", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),
      arrayStructureSchema
    )
    df.printSchema()
    df.show()
    /*
    root
    |-- name: string (nullable = true)
    |-- languagesAtSchool: array (nullable = true)
    |    |-- element: string (containsNull = true)
    |-- languagesAtWork: array (nullable = true)
    |    |-- element: string (containsNull = true)
    |-- currentState: string (nullable = true)
    |-- previousState: string (nullable = true)
    +----------------+------------------+---------------+------------+-------------+
    |            name| languagesAtSchool|languagesAtWork|currentState|previousState|
    +----------------+------------------+---------------+------------+-------------+
    |    James,,Smith|[Java, Scala, C++]|  [Spark, Java]|          OH|           CA|
    |   Michael,Rose,|[Spark, Java, C++]|  [Spark, Java]|          NY|           NJ|
    |Robert,,Williams|      [CSharp, VB]|[Spark, Python]|          UT|           NV|
    +----------------+------------------+---------------+------------+-------------+
     */

    // explode()
    df.select($"name", explode($"languagesAtSchool")).show(false)
    /*
    +----------------+------+
    |name            |col   |
    +----------------+------+
    |James,,Smith    |Java  |
    |James,,Smith    |Scala |
    |James,,Smith    |C++   |
    |Michael,Rose,   |Spark |
    |Michael,Rose,   |Java  |
    |Michael,Rose,   |C++   |
    |Robert,,Williams|CSharp|
    |Robert,,Williams|VB    |
    +----------------+------+
     */

    // split()
    df.select(split($"name", ",").as("nameAsArray")).show(false)
    /*
    +--------------------+
    |nameAsArray         |
    +--------------------+
    |[James, , Smith]    |
    |[Michael, Rose, ]   |
    |[Robert, , Williams]|
    +--------------------+
     */

    // array()
    df.select($"name", array($"currentState", $"previousState").as("States")).show(false)
    /*
    +----------------+--------+
    |name            |States  |
    +----------------+--------+
    |James,,Smith    |[OH, CA]|
    |Michael,Rose,   |[NY, NJ]|
    |Robert,,Williams|[UT, NV]|
    +----------------+--------+
     */

    // array_contains()
    df.select($"name", array_contains($"languagesAtSchool", "Java").as("array_contains")).show()
    /*
    +----------------+--------------+
    |name            |array_contains|
    +----------------+--------------+
    |James,,Smith    |true          |
    |Michael,Rose,   |true          |
    |Robert,,Williams|false         |
    +----------------+--------------+
     */

    // 其他函数可参考 https://sparkbyexamples.com/spark/spark-sql-array-functions/
  }

  private def process2(spark: SparkSession): Unit = {
    // 创建 MapType
    val mapCol = DataTypes.createMapType(StringType, StringType)
    val mapCol2 = DataTypes.createMapType(
      StringType,
      StructType(Array(StructField("col1", StringType), StructField("col2", StringType)))
    )

    // 通过 MapType 该 case class 创建
    val caseMapCol = MapType(StringType, StringType, false)
    val caseMapCol2 = MapType(
      StringType,
      StructType(Array(StructField("col1", StringType), StructField("col2", StringType)))
    )

    import spark.implicits._

    val arrayStructureData = Seq(
      Row(
        "James",
        List(Row("Newark", "NY"), Row("Brooklyn", "NY")),
        Map("hair"   -> "black", "eye" -> "brown"),
        Map("height" -> "5.9")
      ),
      Row(
        "Michael",
        List(Row("SanJose", "CA"), Row("Sandiago", "CA")),
        Map("hair"   -> "brown", "eye" -> "black"),
        Map("height" -> "6")
      ),
      Row(
        "Robert",
        List(Row("LasVegas", "NV")),
        Map("hair"   -> "red", "eye" -> "gray"),
        Map("height" -> "6.3")
      ),
      Row("Maria", null, Map("hair" -> "blond", "eye" -> "red"), Map("height" -> "5.6")),
      Row(
        "Jen",
        List(Row("LAX", "CA"), Row("Orange", "CA")),
        Map("white"  -> "black", "eye" -> "black"),
        Map("height" -> "5.2")
      )
    )

    val mapType = DataTypes.createMapType(StringType, StringType)

    val arrayStructureSchema = new StructType()
      .add("name", StringType)
      .add(
        "addresses",
        ArrayType(
          new StructType()
            .add("city", StringType)
            .add("state", StringType)
        )
      )
      .add("properties", mapType)
      .add("secondProp", MapType(StringType, StringType))

    val mapTypeDF = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),
      arrayStructureSchema
    )
    mapTypeDF.printSchema()
    mapTypeDF.show()
    /*
    root
    |-- name: string (nullable = true)
    |-- addresses: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- city: string (nullable = true)
    |    |    |-- state: string (nullable = true)
    |-- properties: map (nullable = true)
    |    |-- key: string
    |    |-- value: string (valueContainsNull = true)
    |-- secondProp: map (nullable = true)
    |    |-- key: string
    |    |-- value: string (valueContainsNull = true)

    +-------+-------------------------------+------------------------------+---------------+
    |name   |addresses                      |properties                    |secondProp     |
    +-------+-------------------------------+------------------------------+---------------+
    |James  |[[Newark, NY], [Brooklyn, NY]] |[hair -> black, eye -> brown] |[height -> 5.9]|
    |Michael|[[SanJose, CA], [Sandiago, CA]]|[hair -> brown, eye -> black] |[height -> 6]  |
    |Robert |[[LasVegas, NV]]               |[hair -> red, eye -> gray]    |[height -> 6.3]|
    |Maria  |null                           |[hair -> blond, eye -> red]   |[height -> 5.6]|
    |Jen    |[[LAX, CA], [Orange, CA]]      |[white -> black, eye -> black]|[height -> 5.2]|
    +-------+-------------------------------+------------------------------+---------------+
     */

    // 获取所有的键
    mapTypeDF.select($"name", map_keys($"properties")).show(false)
    /*
    +-------+--------------------+
    |name   |map_keys(properties)|
    +-------+--------------------+
    |James  |[hair, eye]         |
    |Michael|[hair, eye]         |
    |Robert |[hair, eye]         |
    |Maria  |[hair, eye]         |
    |Jen    |[white, eye]        |
    +-------+--------------------+
     */

    // 获取所有的值
    mapTypeDF.select($"name", map_values($"properties")).show(false)
    /*
    +-------+----------------------+
    |name   |map_values(properties)|
    +-------+----------------------+
    |James  |[black, brown]        |
    |Michael|[brown, black]        |
    |Robert |[red, gray]           |
    |Maria  |[blond, red]          |
    |Jen    |[black, black]        |
    +-------+----------------------+
     */

    // 结合 maps
    mapTypeDF.select($"name", map_concat($"properties", $"secondProp")).show()
    /*
    +-------+---------------------------------------------+
    |name   |map_concat(properties, secondProp)           |
    +-------+---------------------------------------------+
    |James  |[hair -> black, eye -> brown, height -> 5.9] |
    |Michael|[hair -> brown, eye -> black, height -> 6]   |
    |Robert |[hair -> red, eye -> gray, height -> 6.3]    |
    |Maria  |[hair -> blond, eye -> red, height -> 5.6]   |
    |Jen    |[white -> black, eye -> black, height -> 5.2]|
    +-------+---------------------------------------------+
     */

    // 其他函数可参考 https://sparkbyexamples.com/spark/spark-sql-map-functions/

    // 为 DF 动态创建 MapType
    val structureData = Seq(
      Row("36636", "Finance", Row(3000, "USA")),
      Row("40288", "Finance", Row(5000, "IND")),
      Row("42114", "Sales", Row(3900, "USA")),
      Row("39192", "Marketing", Row(2500, "CAN")),
      Row("34534", "Sales", Row(6500, "USA"))
    )

    val structureSchema = new StructType()
      .add("id", StringType)
      .add("dept", StringType)
      .add(
        "properties",
        new StructType()
          .add("salary", IntegerType)
          .add("location", StringType)
      )

    var df = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)

    val index      = df.schema.fieldIndex("properties")
    val propSchema = df.schema(index).dataType.asInstanceOf[StructType]
    var columns    = LinkedHashSet[Column]()
    propSchema.fields.foreach(field => {
      columns.add(lit(field.name))
      columns.add(col("properties." + field.name))
    })

    df = df.withColumn("propertiesMap", map(columns.toSeq: _*))
    df = df.drop("properties")
    df.printSchema()
    df.show(false)
    /*
    root
    |-- id: string (nullable = true)
    |-- dept: string (nullable = true)
    |-- propertiesMap: map (nullable = false)
    |    |-- key: string
    |    |-- value: string (valueContainsNull = true)

    +-----+---------+---------------------------------+
    |id   |dept     |propertiesMap                    |
    +-----+---------+---------------------------------+
    |36636|Finance  |[salary -> 3000, location -> USA]|
    |40288|Finance  |[salary -> 5000, location -> IND]|
    |42114|Sales    |[salary -> 3900, location -> USA]|
    |39192|Marketing|[salary -> 2500, location -> CAN]|
    |34534|Sales    |[salary -> 6500, location -> USA]|
    +-----+---------+---------------------------------+
     */
  }
}
