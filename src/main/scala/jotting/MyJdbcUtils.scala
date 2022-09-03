package jotting

import java.sql.Connection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

object MyJdbcUtils {

  /** @param df
    * @param statement
    * @param options
    *   key like "columnName1" or key combination string like "columnName1,columnName2", all keys
    *   must have unique constrains
    */
  def runStatement(
      df: DataFrame,
      statement: String,
      options: JdbcOptionsInWrite
  ): Unit = {

    // Listing out all the variables who is required by `.rdd.foreachPartition` closure
    // Otherwise, it will encounter a failed to broadcast variables issue.
    val url            = options.url
    val table          = options.table
    val updateSchema   = df.schema
    val dialect        = JdbcDialects.get(url)
    val batchSize      = options.batchSize
    val isolationLevel = options.isolationLevel

    // System.out.println(s"upsertStmt $statement")
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _                                      => df
    }

    repartitionedDF.rdd.foreachPartition { iterator =>
      JdbcUtils.savePartition(
        table,
        iterator,
        updateSchema,
        statement,
        batchSize,
        dialect,
        isolationLevel,
        options
      )
    }
  }

  object UpsertAction extends Enumeration {
    type UpsertAction = Value
    val DoNothing, DoUpdate = Value
  }

  /** @param table
    * @param tableSchema
    * @param isCaseSensitive
    * @param conflictColumns
    * @param conflictAction
    * @param options
    *   Returns an Insert SQL statement for inserting a row into the target table via JDBC conn.
    */
  private def getUpsertStatement(
      table: String,
      tableSchema: StructType,
      isCaseSensitive: Boolean,
      conflictColumns: Seq[String],
      conflictAction: UpsertAction.Value,
      options: JdbcOptionsInWrite
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
    val tableColumnNames  = tableSchema.fieldNames
    val tableSchemaFields = tableSchema.fields
    val dialect           = JdbcDialects.get(options.url)

    val columns = tableSchemaFields
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
    val placeholders = tableSchemaFields
      .map(_ => "?")
      .mkString(",")

    (options.driverClass, conflictAction) match {
      case ("org.postgresql.Driver", UpsertAction.DoUpdate) =>
        val updateSet = tableColumnNames
          .filterNot(conflictColumns.contains(_))
          .map(n => s""""$n" = EXCLUDED."$n"""")
          .mkString(",")

        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON CONFLICT
          ($conflictString)
        DO UPDATE SET
          $updateSet
        """
      case ("org.postgresql.Driver", _) =>
        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON CONFLICT
          ($conflictString)
        DO NOTHING
        """
      case ("com.mysql.jdbc.Driver", UpsertAction.DoUpdate) =>
        val updateSet = tableColumnNames
          .filterNot(conflictColumns.contains(_))
          .map(n => s""""$n" = VALUES("$n")""")
          .mkString(",")

        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON DUPLICATE KEY UPDATE
          $updateSet
        """
      case ("com.mysql.jdbc.Driver", _) =>
        val updateSet = tableColumnNames
          .filterNot(conflictColumns.contains(_))
          .map(n => s""""$n" = "$n"""")
          .mkString(",")

        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON DUPLICATE KEY UPDATE
          $updateSet
        """
      case _ => throw new Exception("""
        Unsupported driver, upsert method only works on:
          org.postgresql.Driver,
          com.mysql.jdbc.Driver
      """)
    }

  }

  /** @param df
    * @param tableSchema
    * @param isCaseSensitive
    * @param conflictColumns
    * @param options
    *   key like "columnName1" or key combination string like "columnName1,columnName2", all keys
    *   must have unique constrains
    */
  def upsertTable(
      df: DataFrame,
      tableSchema: StructType,
      isCaseSensitive: Boolean,
      conflictColumns: Seq[String],
      conflictAction: UpsertAction.Value,
      options: JdbcOptionsInWrite
  ): Unit = {
    val upsertStmt = getUpsertStatement(
      options.table,
      tableSchema,
      isCaseSensitive,
      conflictColumns,
      conflictAction,
      options
    )

    runStatement(
      df,
      upsertStmt,
      options
    )
  }

  def upsertTable(
      df: DataFrame,
      tableSchema: StructType,
      isCaseSensitive: Boolean,
      conflictColumns: Seq[String],
      conflictAction: String,
      options: JdbcOptionsInWrite
  ): Unit = {

    val ca = conflictAction match {
      case "doNothing" => UpsertAction.DoNothing
      case "doUpdate"  => UpsertAction.DoUpdate
      case _           => throw new Exception("No upsert action matched: doNothing/doUpdate")
    }

    upsertTable(
      df,
      tableSchema,
      isCaseSensitive,
      conflictColumns,
      ca,
      options
    )
  }

  private def connect(options: JdbcOptionsInWrite): Connection = {
    JdbcDialects.get(options.url).createConnectionFactory(options)(-1)
  }

  def dropUniqueConstraint(
      table: String,
      name: String,
      options: JdbcOptionsInWrite
  ): Unit = {
    val query = s"""
    ALTER TABLE
      $table
    DROP CONSTRAINT
      $name
    """

    JdbcUtils.executeQuery(connect(options), options, query)(_ => {})
  }

  def addUniqueConstraint(
      table: String,
      name: String,
      tableColumnNames: Seq[String],
      options: JdbcOptionsInWrite
  ): Unit = {
    val query = s"""
    ALTER TABLE
      $table
    ADD CONSTRAINT
      $name
    UNIQUE
      (${tableColumnNames.mkString(",")})
    """

    JdbcUtils.executeQuery(connect(options), options, query)(_ => {})
  }
}
