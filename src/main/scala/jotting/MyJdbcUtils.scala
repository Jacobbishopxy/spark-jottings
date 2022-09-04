package jotting

import java.sql.Connection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

class MyJdbcUtils(conn: Jotting.Conn) {

  import MyJdbcUtils._

  lazy val dialect = JdbcDialects.get(conn.url)

  /** Create a JdbcOptions
    *
    * @param params
    * @return
    */
  private def genOptions(params: (String, String)*): JdbcOptionsInWrite = {
    val op = params.foldLeft(conn.options) { (acc, ele) =>
      acc + (ele._1 -> ele._2)
    }

    new JdbcOptionsInWrite(op)
  }

  private def jdbcOptionsAddTable(table: String): JdbcOptionsInWrite =
    genOptions(("dbtable", table))

  private def jdbcOptionsAddTruncateTable(
      table: String,
      cascade: Boolean = false
  ): JdbcOptionsInWrite =
    genOptions(
      ("dbtable", table),
      ("cascadeTruncate", cascade.toString())
    )

  private def jdbcOptionsAddSaveTable(
      table: String,
      batchSize: Option[Integer] = None,
      isolationLevel: Option[String] = None,
      numPartition: Option[Integer] = None
  ): JdbcOptionsInWrite = {
    val op = conn.options ++
      batchSize.map(v => ("batchsize" -> v.toString())) ++
      isolationLevel.map(v => ("isolationLevel" -> v)) ++
      numPartition.map(v => ("numPartitions" -> v.toString()))

    new JdbcOptionsInWrite(op)
  }

  private def jdbcOptionsAddCreateTable(
      createTableColumnTypes: Option[String],
      createTableOptions: Option[String],
      tableComment: Option[String]
  ): JdbcOptionsInWrite = {
    val op = conn.options ++
      createTableColumnTypes.map(("createTableColumnTypes" -> _)) ++
      createTableOptions.map(("createTableOptions" -> _)) ++
      tableComment.map(("tableComment" -> _))

    new JdbcOptionsInWrite(op)
  }

  /** Create a connection by extra parameters
    *
    * @param params
    * @return
    */
  private def genConn(params: (String, String)*): Connection = {
    val jdbcOptions = genOptions(params: _*)

    JdbcDialects
      .get(conn.url)
      .createConnectionFactory(jdbcOptions)(-1)
  }

  /** Create a connection by options
    *
    * @param options
    * @return
    */
  private def genConn(options: JdbcOptionsInWrite): Connection = {
    JdbcDialects
      .get(conn.url)
      .createConnectionFactory(options)(-1)
  }

  /** Create a connectionOptions with extra parameters
    *
    * @param params
    * @return
    */
  private def genConnOpt(params: (String, String)*): ConnectionOptions = {
    val jdbcOptions = genOptions(params: _*)

    val connection = JdbcDialects
      .get(conn.url)
      .createConnectionFactory(jdbcOptions)(-1)

    ConnectionOptions(connection, jdbcOptions)
  }

  /** Create a connectionOptions
    *
    * @param options
    * @return
    */
  private def genConnOpt(options: JdbcOptionsInWrite): ConnectionOptions = {
    val connection = JdbcDialects
      .get(conn.url)
      .createConnectionFactory(options)(-1)

    ConnectionOptions(connection, options)
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
    * @param conditions
    * @param isCaseSensitive
    * @param conflictColumns
    * @param conflictAction
    * @return
    */
  private def generateUpsertStatement(
      table: String,
      tableSchema: StructType,
      conditions: Option[String],
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

    val driverClass = genOptions().driverClass
    val stmt = (driverClass, conflictAction) match {
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

    conditions match {
      case Some(value) => stmt ++ s"\nWHERE $value"
      case None        => stmt
    }
  }

  /** Upsert table.
    *
    * Make sure the input DataFrame has the same schema as the database table. Also, unique
    * constraint must be set before calling this method.
    *
    * @param df
    * @param table
    * @param conditions
    * @param isCaseSensitive
    * @param conflictColumns
    * @param conflictAction
    */
  def upsertTable(
      df: DataFrame,
      table: String,
      conditions: Option[String],
      isCaseSensitive: Boolean,
      conflictColumns: Seq[String],
      conflictAction: UpsertAction.Value
  ): Unit = {
    val upsertStmt = generateUpsertStatement(
      table,
      df.schema,
      conditions,
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
    * @param conditions
    * @param isCaseSensitive
    * @param conflictColumns
    * @param conflictAction
    */
  def upsertTable(
      df: DataFrame,
      table: String,
      conditions: Option[String],
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
      conditions,
      isCaseSensitive,
      conflictColumns,
      ca
    )
  }

  private def executeUpdate(
      conn: Connection,
      options: JDBCOptions,
      sql: String
  )(f: Int => Unit): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      val eff = statement.executeUpdate(sql)
      f(eff)
    } finally {
      statement.close()
    }
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
    val co = genConnOpt(jdbcOptionsAddTable(table))

    executeUpdate(co.connection, co.options, query)(_ => {})
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
    val co = genConnOpt(jdbcOptionsAddTable(table))

    executeUpdate(co.connection, co.options, query)(_ => {})
  }

  /** Delete data from a table
    *
    * @param table
    * @param conditions
    */
  def deleteByConditions(table: String, conditions: String): Unit = {
    val query = s"""
    DELETE FROM ${table} WHERE $conditions
    """
    val co = genConnOpt(jdbcOptionsAddTable(table))

    executeUpdate(co.connection, co.options, query)(_ => {})
  }

  /** Check if a table exists
    *
    * @param table
    * @return
    */
  def tableExists(table: String): Boolean = {
    val co = genConnOpt(jdbcOptionsAddTable(table))

    JdbcUtils.tableExists(co.connection, co.options)
  }

  /** Drop a table
    *
    * @param table
    */
  def dropTable(table: String): Unit = {
    val co = genConnOpt(jdbcOptionsAddTable(table))

    JdbcUtils.dropTable(co.connection, table, co.options)
  }

  /** Truncate a table
    *
    * @param table
    * @param cascadeTruncate
    */
  def truncateTable(table: String, cascadeTruncate: Boolean = false): Unit = {
    val co = genConnOpt(jdbcOptionsAddTruncateTable(table, cascadeTruncate))

    JdbcUtils.truncateTable(co.connection, co.options)
  }

  /** Get a table's schema if it exists
    *
    * @param table
    * @return
    */
  def getTableSchema(table: String): Option[StructType] = {
    val co = genConnOpt(jdbcOptionsAddTable(table))

    JdbcUtils.getSchemaOption(co.connection, co.options)
  }

  /** Save a DataFrame to the database in a single transaction
    *
    * @param df
    * @param table
    * @param isCaseSensitive
    * @param batchSize
    * @param isolationLevel
    * @param numPartition
    */
  def saveTable(
      df: DataFrame,
      table: String,
      isCaseSensitive: Boolean,
      batchSize: Option[Integer] = None,
      isolationLevel: Option[String] = None,
      numPartition: Option[Integer] = None
  ): Unit =
    JdbcUtils.saveTable(
      df,
      None,
      isCaseSensitive,
      jdbcOptionsAddSaveTable(
        table,
        batchSize,
        isolationLevel,
        numPartition
      )
    )

  /** Create a table with a given schema
    *
    * @param table
    * @param schema
    * @param isCaseSensitive
    * @param createTableColumnTypes
    * @param createTableOptions
    * @param tableComment
    */
  def createTable(
      table: String,
      schema: StructType,
      isCaseSensitive: Boolean,
      createTableColumnTypes: Option[String],
      createTableOptions: Option[String],
      tableComment: Option[String]
  ): Unit = {
    val co = genConnOpt(
      jdbcOptionsAddCreateTable(
        createTableColumnTypes,
        createTableOptions,
        tableComment
      )
    )
    JdbcUtils.createTable(
      co.connection,
      table,
      schema,
      isCaseSensitive,
      co.options
    )
  }

  /** Rename a table from the JDBC database
    *
    * @param oldTable
    * @param newTable
    */
  def renameTable(oldTable: String, newTable: String): Unit = {
    val c = genConnOpt()

    JdbcUtils.renameTable(c.connection, oldTable, newTable, c.options)
  }

  /** Update a table from the JDBC database
    *
    * @param table
    * @param changes
    */
  def alterTable(table: String, changes: Seq[TableChange]): Unit = {
    val c = genConnOpt()

    JdbcUtils.alterTable(c.connection, table, changes, c.options)
  }
  // TODO:
  // createSchema
  // schemaExists
  // listSchemas
  // alterSchemaComment
  // removeSchemaComment
  // dropSchema

  // TODO:
  // createIndex
  // indexExists
  // dropIndex
  // listIndexes
  // checkIfIndexExists
}

object MyJdbcUtils {

  /** Upsert action's enum
    */
  object UpsertAction extends Enumeration {
    type UpsertAction = Value
    val DoNothing, DoUpdate = Value
  }

  case class ConnectionOptions(connection: Connection, options: JdbcOptionsInWrite)
}
