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
    def url: String = s"jdbc:$db://$host:$port/$database?zeroDateTimeBehavior=convertToNull"

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
