package jotting

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
}
