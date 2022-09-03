package jotting

import java.sql.Connection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

class MyJdbcUtils(conn: Jotting.Conn, connectionOpt: Option[Connection] = None) {

  import MyJdbcUtils._

  lazy val connection = connectionOpt.getOrElse(
    JdbcDialects
      .get(conn.url)
      .createConnectionFactory(new JdbcOptionsInWrite(conn.options))(-1)
  )
  lazy val jdbcOptions = new JdbcOptionsInWrite(conn.options)
  lazy val dialect     = JdbcDialects.get(jdbcOptions.url)

  private def jdbcOptionsAddOptions(params: (String, String)*): JdbcOptionsInWrite = {
    val op = params.foldLeft(conn.options) { (acc, ele) =>
      acc + (ele._1 -> ele._2)
    }

    new JdbcOptionsInWrite(op)
  }

  private def jdbcOptionsAddTable(table: String): JdbcOptionsInWrite = {
    jdbcOptionsAddOptions(("dbtable", table))
  }

  private def jdbcOptionsAddTruncateTable(
      table: String,
      cascade: Boolean = false
  ): JdbcOptionsInWrite = {
    jdbcOptionsAddOptions(("dbtable", table), ("cascadeTruncate", cascade.toString()))
  }

  /** Raw statement for saving a DataFrame
    *
    * @param df
    * @param statement
    * @param options
    */
  def runStatement(
      df: DataFrame,
      statement: String,
      options: JdbcOptionsInWrite
  ): Unit = {
    val url            = options.url
    val table          = options.table
    val updateSchema   = df.schema
    val batchSize      = options.batchSize
    val isolationLevel = options.isolationLevel

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

  /** Generate an upsert SQL statement.
    *
    * Currently, only supports MySQL & PostgreSQL
    *
    * @param table
    * @param tableSchema
    * @param isCaseSensitive
    * @param conflictColumns
    * @param conflictAction
    * @return
    */
  private def generateUpsertStatement(
      table: String,
      tableSchema: StructType,
      isCaseSensitive: Boolean,
      conflictColumns: Seq[String],
      conflictAction: UpsertAction.Value
  ): String = {
    val columnNameEquality = if (isCaseSensitive) {
      org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    } else {
      org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    }
    val tableColumnNames  = tableSchema.fieldNames
    val tableSchemaFields = tableSchema.fields

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

    (jdbcOptions.driverClass, conflictAction) match {
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
      case _ =>
        throw new Exception(
          "Unsupported driver, upsert method only works on: org.postgresql.Driver/com.mysql.jdbc.Driver"
        )
    }

  }

  /** Upsert table.
    *
    * Make sure the input DataFrame has the same schema as the database table. Also, unique
    * constraint must be set before calling this method.
    *
    * @param df
    * @param table
    * @param isCaseSensitive
    * @param conflictColumns
    * @param conflictAction
    */
  def upsertTable(
      df: DataFrame,
      table: String,
      isCaseSensitive: Boolean,
      conflictColumns: Seq[String],
      conflictAction: UpsertAction.Value
  ): Unit = {
    val upsertStmt = generateUpsertStatement(
      table,
      df.schema,
      isCaseSensitive,
      conflictColumns,
      conflictAction
    )

    runStatement(
      df,
      upsertStmt,
      jdbcOptionsAddTable(table)
    )
  }

  /** Upsert table.
    *
    * Make sure the input DataFrame has the same schema as the database table.
    *
    * @param df
    * @param table
    * @param isCaseSensitive
    * @param conflictColumns
    * @param conflictAction
    */
  def upsertTable(
      df: DataFrame,
      table: String,
      isCaseSensitive: Boolean,
      conflictColumns: Seq[String],
      conflictAction: String
  ): Unit = {

    val ca = conflictAction match {
      case "doNothing" => UpsertAction.DoNothing
      case "doUpdate"  => UpsertAction.DoUpdate
      case _           => throw new Exception("No upsert action matched: doNothing/doUpdate")
    }

    upsertTable(
      df,
      table,
      isCaseSensitive,
      conflictColumns,
      ca
    )
  }

  /** Drop a unique constraint from a table
    *
    * @param table
    * @param name
    */
  def dropUniqueConstraint(table: String, name: String): Unit = {
    val query = s"""
    ALTER TABLE
      $table
    DROP CONSTRAINT
      $name
    """

    JdbcUtils.executeQuery(connection, jdbcOptions, query)(_ => {})
  }

  /** Create a unique constraint for a table
    *
    * @param table
    * @param name
    * @param tableColumnNames
    */
  def addUniqueConstraint(
      table: String,
      name: String,
      tableColumnNames: Seq[String]
  ): Unit = {
    val query = s"""
    ALTER TABLE
      $table
    ADD CONSTRAINT
      $name
    UNIQUE
      (${tableColumnNames.mkString(",")})
    """

    JdbcUtils.executeQuery(connection, jdbcOptions, query)(_ => {})
  }

  def tableExists(table: String): Boolean =
    JdbcUtils.tableExists(connection, jdbcOptionsAddTable(table))

  def dropTable(table: String): Unit =
    JdbcUtils.dropTable(connection, table, jdbcOptions)

  def truncateTable(table: String, cascadeTruncate: Boolean = false): Unit =
    JdbcUtils.truncateTable(connection, jdbcOptionsAddTruncateTable(table, cascadeTruncate))

  def getSchemaOption(table: String): Option[StructType] = {
    JdbcUtils.getSchemaOption(connection, jdbcOptionsAddTable(table))
  }

  // TODO:
}

object MyJdbcUtils {

  /** Upsert action's enum
    */
  object UpsertAction extends Enumeration {
    type UpsertAction = Value
    val DoNothing, DoUpdate = Value
  }
}
