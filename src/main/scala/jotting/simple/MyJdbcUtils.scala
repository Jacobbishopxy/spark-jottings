package jotting.simple

import java.sql.Connection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.StructType

object MyJdbcUtils {

  /** @param df
    * @param tableSchema
    * @param isCaseSensitive
    * @param options
    * @param conflictColumns
    *   key like "columnName1" or key combination string like "columnName1,columnName2", all keys
    *   must have unique constrains
    */
  def runStatement(
      df: DataFrame,
      tableSchema: StructType,
      isCaseSensitive: Boolean,
      options: JdbcOptionsInWrite,
      statement: String
  ): Unit = {
    val url            = options.url
    val table          = options.table
    val dialect        = JdbcDialects.get(url)
    val batchSize      = options.batchSize
    val isolationLevel = options.isolationLevel

    val upsertStmt = statement
    // System.out.println(s"upsertStmt $upsertStmt")
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _                                      => df
    }
    val updateSchema = df.schema
    repartitionedDF.rdd.foreachPartition { iterator =>
      JdbcUtils.savePartition(
        table,
        iterator,
        updateSchema,
        upsertStmt,
        batchSize,
        dialect,
        isolationLevel,
        options
      )
    }
  }

  /** @param df
    * @param tableSchema
    * @param isCaseSensitive
    * @param options
    * @param conflictColumns
    *   key like "columnName1" or key combination string like "columnName1,columnName2", all keys
    *   must have unique constrains
    */
  def upsertTable(
      df: DataFrame,
      tableSchema: StructType,
      isCaseSensitive: Boolean,
      options: JdbcOptionsInWrite,
      conflictColumns: Seq[String]
  ): Unit = {
    val url            = options.url
    val table          = options.table
    val dialect        = JdbcDialects.get(url)
    val batchSize      = options.batchSize
    val isolationLevel = options.isolationLevel

    val upsertStmt = getUpsertStatement(
      table,
      tableSchema,
      isCaseSensitive,
      dialect,
      conflictColumns
    )
    System.out.println(s"upsertStmt $upsertStmt")
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _                                      => df
    }
    val updateSchema = df.schema
    repartitionedDF.rdd.foreachPartition { iterator =>
      JdbcUtils.savePartition(
        table,
        iterator,
        updateSchema,
        upsertStmt,
        batchSize,
        dialect,
        isolationLevel,
        options
      )
    }
  }

  /** Returns an Insert SQL statement for inserting a row into the target table via JDBC conn.
    */
  def getUpsertStatement(
      table: String,
      tableSchema: StructType,
      isCaseSensitive: Boolean,
      dialect: JdbcDialect,
      conflictColumns: Seq[String]
  ): String = {
    val columnNameEquality = if (isCaseSensitive) {
      org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    } else {
      org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    }
    // The generated insert statement needs to follow rddSchema's column sequence and
    // tableSchema's column names. When appending data into some case-sensitive DBMSs like
    // PostgreSQL/Oracle, we need to respect the existing case-sensitive column names instead of
    // RDD column names for user convenience.
    val tableColumnNames = tableSchema.fieldNames
    val columns = tableSchema.fields
      .map { col =>
        val normalizedName = tableColumnNames
          .find(columnNameEquality(_, col.name))
          .get
        dialect.quoteIdentifier(normalizedName)
      }
      .mkString(",")

    val conflictString = conflictColumns
      .map(x => s""""$x"""")
      .mkString(",")
    val placeholders = tableSchema.fields
      .map(_ => "?")
      .mkString(",")
    val updateColumns = tableSchema.fieldNames
      .filterNot(conflictColumns.contains(_))
      .map(x => s""""$x"""")
    val updateColumnString = updateColumns.mkString(",")
    val updatePlaceholder = tableSchema.fieldNames
      .filterNot(conflictColumns.contains(_))
      .map(_ => "?")
      .mkString(",")

    updateColumns.length match {
      case 0 =>
        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON CONFLICT
          ($conflictString)
        DO NOTHING
        """
      case _ =>
        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON CONFLICT
          ($conflictString)
        DO UPDATE SET
          ($updateColumnString) = ROW($updatePlaceholder)
        """
    }
  }
}
