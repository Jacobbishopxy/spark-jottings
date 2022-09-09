package jotting

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType}
import org.apache.spark.sql.SaveMode

/** Get the latest update time from the target table, and query the rest of data from the resource
  * table.
  */
object RegimeTimeHelper {

  import Ordering.Implicits._

  private def getMaxDate(columnName: String, tableName: String) = s"""
    SELECT MAX($columnName) FROM $tableName
    """

  private def checkLastUpdateTime[T](
      helper: RegimeJdbcHelper,
      sql: String
  )(implicit spark: SparkSession): Option[T] =
    try {
      Some(helper.readTable(sql).first().get(0).asInstanceOf[T])
    } catch {
      case _: Throwable => None
    }

  private def shouldInsertFrom[T: Ordering](
      sourceTime: Option[T],
      targetTime: Option[T]
  ): Option[T] = {
    (targetTime, sourceTime) match {
      case (Some(st), Some(tt)) if st > tt => targetTime
      case _                               => None
    }
  }

  def insertFromLastUpdateTime[T: Ordering](
      sourceConn: Jotting.Conn,
      sourceColumn: String,
      sourceTable: String,
      targetConn: Jotting.Conn,
      targetColumn: String,
      targetTable: String,
      querySqlCst: T => String
  )(implicit spark: SparkSession): Unit = {
    val sourceHelper = RegimeJdbcHelper(sourceConn)
    val targetHelper = RegimeJdbcHelper(targetConn)

    val sourceTime = checkLastUpdateTime[T](sourceHelper, getMaxDate(sourceColumn, sourceTable))
    val targetTime = checkLastUpdateTime[T](targetHelper, getMaxDate(targetColumn, targetTable))

    val updateFrom = shouldInsertFrom(targetTime, sourceTime)

    updateFrom.map { time =>
      val df = sourceHelper.readTable(querySqlCst(time))
      sourceHelper.saveTable(df, targetTable, SaveMode.Append)
    }
  }
}
