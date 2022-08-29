package jotting.example

import org.apache.spark.sql.SparkSession

object DFCacheAndPersist {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame cache and persist example")
      .getOrCreate()

    process(spark)

    spark.stop()
  }

  /*
  transformations 皆为 lazy 操作；actions 皆为 eger 操作。

  - transformations 定义与划分：
    https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations

  - transformations 操作：
    - map
    - filter
    - flatMap
    - mapPartitions
    - mapPartitionsWithIndex
    - sample
    - union
    - intersection
    - distinct
    - groupByKey
    - reduceByKey
    - aggregateByKey
    - sortByKey
    - join
    - cogroup
    - cartesian
    - pipe
    - coalesce
    - repartition
    - repartitionAndSortWithinPartitions

  - actions 定义与划分：
    https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions

  - actions 操作：
    - reduce
    - collect
    - count
    - first
    - take
    - takeSample
    - takeOrdered
    - saveAsTextFile
    - saveAsSequenceFile
    - saveAsObjectFile
    - countByKey
    - foreach

  - Persist 的 storage levels：
    - MEMORY_ONLY： This is the default behavior of the RDD cache() method and stores the RDD or DataFrame as deserialized objects to JVM memory. When there is not enough memory available it will not save DataFrame of some partitions and these will be re-computed as and when required. This takes more memory. but unlike RDD, this would be slower than MEMORY_AND_DISK level as it recomputes the unsaved partitions, and recomputing the in-memory columnar representation of the underlying table is expensive
    - MEMORY_ONLY_SER： This is the same as MEMORY_ONLY but the difference being it stores RDD as serialized objects to JVM memory. It takes lesser memory (space-efficient) than MEMORY_ONLY as it saves objects as serialized and takes an additional few more CPU cycles in order to deserialize.
    - MEMORY_ONLY_2： Same as MEMORY_ONLY storage level but replicate each partition to two cluster nodes.
    - MEMORY_ONLY_SER_2： Same as MEMORY_ONLY_SER storage level but replicate each partition to two cluster nodes.
    - MEMORY_AND_DISK： This is the default behavior of the DataFrame or Dataset. In this Storage Level, The DataFrame will be stored in JVM memory as a deserialized object. When required storage is greater than available memory, it stores some of the excess partitions into a disk and reads the data from the disk when required. It is slower as there is I/O involved.
    - MEMORY_AND_DISK_SER： This is the same as MEMORY_AND_DISK storage level difference being it serializes the DataFrame objects in memory and on disk when space is not available.
    - MEMORY_AND_DISK_2： Same as MEMORY_AND_DISK storage level but replicate each partition to two cluster nodes.
    - MEMORY_AND_DISK_SER_2： Same as MEMORY_AND_DISK_SER storage level but replicate each partition to two cluster nodes.
    - DISK_ONLY： In this storage level, DataFrame is stored only on disk and the CPU computation time is high as I/O is involved.
    - DISK_ONLY_2： Same as DISK_ONLY storage level but replicate each partition to two cluster nodes.

   */
  private def process(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.json("src/main/resources/employees.json")

    // 此处为缓存标记，为 lazy 操作
    df.cache()
    // 如果使用 persist() 函数，请详细查看 StorageLevel 选项
    import org.apache.spark.storage.StorageLevel
    // df.persist(StorageLevel.MEMORY_AND_DISK)

    // 此处为 transformation，为 lazy 操作
    val df2 = df.where($"name".isin("Andy", "Justin"))
    // 此处为 action，为 eger 操作，即消耗 df 的内存
    df2.show()

    val df3 = df.filter($"salary" >= 4000)
    // 此处为 action，如果未进行 cache 操作，则会重新读取 json
    df3.show()

    // 释放缓存，为 eger 操作
    df.unpersist()
  }

}
