package jotting

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

/** Get the latest update time from the target table, and query the rest of data from the resource
  * table.
  */
object RegimeTimeHelper {
  private def getMaxDate(columnName: String, tableName: String) = s"""
    SELECT MAX($columnName) AS max_$columnName FROM $tableName
    """

  def insertFromLastUpdateTime(
      sourceConn: Jotting.Conn,
      sourceColumn: String,
      sourceTable: String,
      targetConn: Jotting.Conn,
      targetColumn: String,
      targetTable: String,
      querySqlCst: Any => String
  )(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val sourceHelper = RegimeJdbcHelper(sourceConn)
    val targetHelper = RegimeJdbcHelper(targetConn)

    val sourceDf = sourceHelper.readTable(getMaxDate(sourceColumn, sourceTable))
    val targetDf = targetHelper.readTable(getMaxDate(targetColumn, targetTable))

    sourceDf.createOrReplaceTempView("SOURCE_DF")
    targetDf.createOrReplaceTempView("TARGET_DF")

    val resRow = spark
      .sql(s"""
    SELECT if(
      (SELECT first_value(max_$sourceColumn) FROM SOURCE_DF) > (SELECT first_value(max_$targetColumn) FROM TARGET_DF),
      (SELECT first_value(max_$targetColumn) FROM TARGET_DF),
      NULL
    )
    """)
      .toDF()
      .first()

    if (resRow.isNullAt(0)) {
      return
    }

    val updateFrom = resRow.get(0)

    val df = sourceHelper.readTable(querySqlCst(updateFrom))
    sourceHelper.saveTable(df, targetTable, SaveMode.Append)
  }
}
