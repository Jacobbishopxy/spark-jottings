package jotting

import java.sql.Connection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.catalog.index.TableIndex
import org.apache.spark.sql.SaveMode

class MyJdbcUtils(conn: Jotting.Conn) {

  import MyJdbcUtils._

  // ===============================================================================================
  // private helper functions
  // ===============================================================================================

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
    val op = conn.options +
      ("dbtable" -> table) ++
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

  /** Execute a raw SQL statement
    *
    * @param conn
    * @param options
    * @param sql
    * @param f
    */
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

  private def genUnsupportedDriver(method: String): String =
    s"Unsupported driver, $method method only works on: org.postgresql.Driver/com.mysql.jdbc.Driver"

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
    val dialect           = JdbcDialects.get(conn.url)

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

    val stmt = (conn.driver, conflictAction) match {
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
        throw new Exception(genUnsupportedDriver("upsert"))
    }

    conditions match {
      case Some(value) => stmt ++ s"\nWHERE $value"
      case None        => stmt
    }
  }

  /** Generate a createIndex SQL statement
    *
    * @param table
    * @param name
    * @param columns
    * @return
    */
  private def generateCreateIndexStatement(
      table: String,
      indexName: String,
      columns: Seq[String]
  ): String =
    conn.driver match {
      case "org.postgresql.Driver" =>
        val cl = columns.map(c => s""""$c"""").mkString(",")
        s"""
        CREATE INDEX "$indexName" ON "$table" ($cl)
        """
      case "com.mysql.jdbc.Driver" =>
        val cl = columns.map(c => s"""`$c`""").mkString(",")
        s"""
        CREATE INDEX `$indexName` ON `$table` ($cl)
        """
      case _ =>
        throw new Exception(genUnsupportedDriver("createIndex"))
    }

  /** Generate a dropIndex SQL statement
    *
    * @param table
    * @param name
    * @return
    */
  private def generateDropIndexStatement(table: String, name: String): String =
    conn.driver match {
      case "org.postgresql.Driver" =>
        s"""
        DROP INDEX "$name"
        """
      case "com.mysql.jdbc.Driver" =>
        s"""
        DROP INDEX `$name` ON `$table`
        """
      case _ =>
        throw new Exception(genUnsupportedDriver("dropIndex"))
    }

  /** Generate a createForeignKey SQL statement
    *
    * @param fromTable
    * @param fromTableColumn
    * @param foreignKeyName
    * @param toTable
    * @param toTableColumn
    * @param onDelete
    * @param onUpdate
    * @return
    */
  private def generateCreateForeignKey(
      fromTable: String,
      fromTableColumn: String,
      foreignKeyName: String,
      toTable: String,
      toTableColumn: String,
      onDelete: Option[ForeignKeyModifyAction.Value],
      onUpdate: Option[ForeignKeyModifyAction.Value]
  ): String =
    conn.driver match {
      case "org.postgresql.Driver" =>
        s"""
        ALTER TABLE "$fromTable"
        ADD CONSTRAINT "$foreignKeyName"
        FOREIGN KEY ("$fromTableColumn")
        REFERENCES "$toTable" ("$toTableColumn")
        """ +
          onDelete
            .map(ForeignKeyModifyAction.generateString(_, DeleteOrUpdate.Delete))
            .getOrElse("") +
          onUpdate
            .map(ForeignKeyModifyAction.generateString(_, DeleteOrUpdate.Update))
            .getOrElse("")

      case "com.mysql.jdbc.Driver" =>
        s"""
        ALTER TABLE `$fromTable`
        ADD CONSTRAINT `$foreignKeyName`
        FOREIGN KEY (`$fromTableColumn`)
        REFERENCES `$toTable` (`$toTableColumn`)
        """ +
          onDelete
            .map(ForeignKeyModifyAction.generateString(_, DeleteOrUpdate.Delete))
            .getOrElse("") +
          onUpdate
            .map(ForeignKeyModifyAction.generateString(_, DeleteOrUpdate.Update))
            .getOrElse("")
      case _ =>
        throw new Exception(genUnsupportedDriver("createForeignKey"))
    }

  /** Generate a dropForeignKey SQL statement
    *
    * @param table
    * @param foreignKeyName
    * @return
    */
  private def generateDropForeignKey(table: String, foreignKeyName: String): String = {
    conn.driver match {
      case "org.postgresql.Driver" =>
        s"""
        ALTER TABLE "$table" DROP CONSTRAINT "$foreignKeyName"
        """
      case "com.mysql.jdbc.Driver" =>
        s"""
        ALTER TABLE `$table` DROP FOREIGN KEY "$foreignKeyName"
        """
      case _ =>
        throw new Exception(genUnsupportedDriver("dropForeignKey"))

    }
  }

  // ===============================================================================================
  // general functions
  // 1. runStatement
  // 1. saveTable
  // 1. upsertTable
  // 1.
  // ===============================================================================================

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
    // IMPORTANT!
    // These variables are required under this context scope, because of `.rdd.foreachPartition` broadcasting.
    val url            = options.url
    val dialect        = JdbcDialects.get(url)
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

    executeUpdate(co.conn, co.opt, query)(_ => {})
  }

  /** Check if a table exists
    *
    * @param table
    * @return
    */
  def tableExists(table: String): Boolean = {
    val co = genConnOpt(jdbcOptionsAddTable(table))

    JdbcUtils.tableExists(co.conn, co.opt)
  }

  /** Drop a table
    *
    * @param table
    */
  def dropTable(table: String): Unit = {
    val co = genConnOpt(jdbcOptionsAddTable(table))

    JdbcUtils.dropTable(co.conn, table, co.opt)
  }

  /** Truncate a table
    *
    * @param table
    * @param cascadeTruncate
    */
  def truncateTable(table: String, cascadeTruncate: Boolean = false): Unit = {
    val co = genConnOpt(jdbcOptionsAddTruncateTable(table, cascadeTruncate))

    JdbcUtils.truncateTable(co.conn, co.opt)
  }

  /** Get a table's schema if it exists
    *
    * @param table
    * @return
    */
  def getTableSchema(table: String): Option[StructType] = {
    val co = genConnOpt(jdbcOptionsAddTable(table))

    JdbcUtils.getSchemaOption(co.conn, co.opt)
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

  /** Save a DataFrame to the database with mode option
    *
    * @param df
    * @param table
    * @param mode
    */
  def saveTable(
      df: DataFrame,
      table: String,
      mode: SaveMode
  ): Unit = {
    df.write
      .format("jdbc")
      .options(conn.options)
      .option("dbtable", table)
      .mode(mode)
      .save()
  }

  def saveTable(
      df: DataFrame,
      table: String,
      mode: String
  ): Unit = {
    df.write
      .format("jdbc")
      .options(conn.options)
      .option("dbtable", table)
      .mode(mode)
      .save()
  }

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
      co.conn,
      table,
      schema,
      isCaseSensitive,
      co.opt
    )
  }

  /** Rename a table from the JDBC database
    *
    * @param oldTable
    * @param newTable
    */
  def renameTable(oldTable: String, newTable: String): Unit = {
    val co = genConnOpt()

    JdbcUtils.renameTable(co.conn, oldTable, newTable, co.opt)
  }

  /** Update a table from the JDBC database
    *
    * @param table
    * @param changes
    */
  def alterTable(table: String, changes: Seq[TableChange]): Unit = {
    val co = genConnOpt(jdbcOptionsAddTable(table))

    JdbcUtils.alterTable(co.conn, table, changes, co.opt)
  }

  /** Create a schema
    *
    * @param schema
    * @param comment
    */
  def createSchema(schema: String, comment: String): Unit = {
    val co = genConnOpt()

    JdbcUtils.createSchema(co.conn, co.opt, schema, comment)
  }

  /** Check a schema if exists
    *
    * @param schema
    * @return
    */
  def schemaExists(schema: String): Boolean = {
    val co = genConnOpt()

    JdbcUtils.schemaExists(co.conn, co.opt, schema)
  }

  /** List all schema
    *
    * @return
    */
  def listSchemas(): Array[Array[String]] = {
    val co = genConnOpt()

    JdbcUtils.listSchemas(co.conn, co.opt)
  }

  /** Alter a schema's comment
    *
    * @param schema
    * @param comment
    */
  def alterSchemaComment(schema: String, comment: String): Unit = {
    val co = genConnOpt()

    JdbcUtils.alterSchemaComment(co.conn, co.opt, schema, comment)
  }

  /** Remove a schema's comment
    *
    * @param schema
    */
  def removeSchemaComment(schema: String): Unit = {
    val co = genConnOpt()

    JdbcUtils.removeSchemaComment(co.conn, co.opt, schema)
  }

  /** Drop a schema
    *
    * @param schema
    * @param cascade
    */
  def dropSchema(schema: String, cascade: Boolean): Unit = {
    val co = genConnOpt()

    JdbcUtils.dropSchema(co.conn, co.opt, schema, cascade)
  }

  /** Create a unique constraint for a table
    *
    * @param table
    * @param name
    * @param tableColumnNames
    */
  def createUniqueConstraint(
      table: String,
      name: String,
      tableColumnNames: Seq[String]
  ): Unit = {
    val query = s"""
    ALTER TABLE $table ADD CONSTRAINT $name
    UNIQUE (${tableColumnNames.mkString(",")})
    """
    val co = genConnOpt(jdbcOptionsAddTable(table))

    executeUpdate(co.conn, co.opt, query)(_ => {})
  }

  /** Drop a unique constraint from a table
    *
    * @param table
    * @param name
    */
  def dropUniqueConstraint(table: String, name: String): Unit = {
    val query = s"""
    ALTER TABLE $table DROP CONSTRAINT $name
    """
    val co = genConnOpt(jdbcOptionsAddTable(table))

    executeUpdate(co.conn, co.opt, query)(_ => {})
  }

  /** Create an index
    *
    * @param table
    * @param index
    * @param columns
    * @param columnsProperties
    * @param properties
    */
  def createIndex(
      table: String,
      index: String,
      columns: Seq[String]
  ): Unit = {
    val co    = genConnOpt(jdbcOptionsAddTable(table))
    val query = generateCreateIndexStatement(table, index, columns)

    executeUpdate(co.conn, co.opt, query)(_ => {})
  }

  /** Check an index if exists
    *
    * @param table
    * @param index
    * @return
    */
  def indexExists(table: String, index: String): Boolean = {
    throw new UnsupportedOperationException("indexExists not supported currently")
  }

  /** Drop an index
    *
    * @param table
    * @param index
    */
  def dropIndex(table: String, index: String): Unit = {
    val query = generateDropIndexStatement(table, index)
    val co    = genConnOpt(jdbcOptionsAddTable(table))

    executeUpdate(co.conn, co.opt, query)(_ => {})
  }

  /** List indices from a table
    *
    * @param table
    * @return
    */
  def listIndexes(table: String): Array[TableIndex] = {
    throw new UnsupportedOperationException("listIndexes not supported currently")
  }

  /** Create a foreign key
    *
    * @param fromTable
    * @param fromTableColumn
    * @param foreignKeyName
    * @param toTable
    * @param toTableColumn
    * @param onDelete
    * @param onUpdate
    */
  def createForeignKey(
      fromTable: String,
      fromTableColumn: String,
      foreignKeyName: String,
      toTable: String,
      toTableColumn: String,
      onDelete: Option[ForeignKeyModifyAction.Value],
      onUpdate: Option[ForeignKeyModifyAction.Value]
  ): Unit = {
    val query = generateCreateForeignKey(
      fromTable,
      fromTableColumn,
      foreignKeyName,
      toTable,
      toTableColumn,
      onDelete,
      onUpdate
    )
    val co = genConnOpt()

    executeUpdate(co.conn, co.opt, query)(_ => {})
  }

  /** Drop a foreign key
    *
    * @param table
    * @param name
    */
  def dropForeignKey(table: String, name: String): Unit = {
    val query = generateDropForeignKey(table, name)
    val co    = genConnOpt()

    executeUpdate(co.conn, co.opt, query)(_ => {})
  }

}

object MyJdbcUtils {

  /** Upsert action's enum
    */
  object UpsertAction extends Enumeration {
    type UpsertAction = Value
    val DoNothing, DoUpdate = Value
  }

  /** OnDelete/OnUpdate. Private marker
    */
  private object DeleteOrUpdate extends Enumeration {
    type DeleteOrUpdate = Value
    val Delete, Update = Value

    def generateString(v: Value): String = v match {
      case Delete => "DELETE"
      case Update => "UPDATE"
    }
  }

  /** ForeignKey onDelete/onUpdate's actions
    */
  object ForeignKeyModifyAction extends Enumeration {
    type ForeignKeyModifyAction = Value
    val SetNull, SetDefault, Restrict, NoAction, Cascade = Value

    def generateString(a: Value, t: DeleteOrUpdate.Value): String =
      a match {
        case ForeignKeyModifyAction.SetNull =>
          s"\nON ${DeleteOrUpdate.generateString(t)} SET NULL"
        case ForeignKeyModifyAction.SetDefault =>
          s"\nON ${DeleteOrUpdate.generateString(t)} SET DEFAULT"
        case ForeignKeyModifyAction.Restrict =>
          s"\nON ${DeleteOrUpdate.generateString(t)} RESTRICT"
        case ForeignKeyModifyAction.NoAction =>
          s"\nON ${DeleteOrUpdate.generateString(t)} NO ACTION"
        case ForeignKeyModifyAction.Cascade =>
          s"\nON ${DeleteOrUpdate.generateString(t)} CASCADE"
      }
  }

  case class ConnectionOptions(conn: Connection, opt: JdbcOptionsInWrite)

  def apply(conn: Jotting.Conn): MyJdbcUtils = new MyJdbcUtils(conn)
}
