package jotting

import com.typesafe.config.Config

object Jotting {
  val configFile = "application.conf"

  case class Conn(
      db: String,
      driver: String,
      host: String,
      port: Int,
      database: String,
      user: String,
      password: String
  ) {
    lazy val driverType = driver match {
      case "com.microsoft.sqlserver.jdbc.SQLServerDriver" => DriverType.MsSql
      case "com.mysql.jdbc.Driver"                        => DriverType.MySql
      case "org.postgresql.Driver"                        => DriverType.Postgres
      case _                                              => DriverType.Other
    }

    def url: String = {
      driverType match {
        case DriverType.MsSql =>
          s"jdbc:$db://$host:$port;databaseName=$database;encrypt=true;trustServerCertificate=true;"
        case _ =>
          s"jdbc:$db://$host:$port/$database"
      }
    }

    def options: Map[String, String] = Map(
      "url"      -> this.url,
      "driver"   -> driver,
      "user"     -> user,
      "password" -> password
    )
  }

  object Conn {
    def apply(config: Config): Conn =
      Conn(
        config.getString("db"),
        config.getString("driver"),
        config.getString("host"),
        config.getInt("port"),
        config.getString("database"),
        config.getString("user"),
        config.getString("password")
      )
  }
}

object DriverType extends Enumeration {
  type DriverType = Value
  val Postgres, MySql, MsSql, Other = Value
}
