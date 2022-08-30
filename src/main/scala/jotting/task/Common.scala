package jotting.task

import com.typesafe.config.ConfigFactory

import jotting.Jotting

object Common {
  private val config        = ConfigFactory.load(Jotting.configFile).getConfig("taskCommon")
  private val configCapRaw  = config.getConfig("capRaw")
  private val configCapProd = config.getConfig("capProd")

  val connCapRaw = Jotting.Conn(
    configCapRaw.getString("db"),
    configCapRaw.getString("driver"),
    configCapRaw.getString("host"),
    configCapRaw.getInt("port"),
    configCapRaw.getString("database"),
    configCapRaw.getString("user"),
    configCapRaw.getString("password")
  )
  val connCapProd = Jotting.Conn(
    configCapProd.getString("db"),
    configCapProd.getString("driver"),
    configCapProd.getString("host"),
    configCapProd.getInt("port"),
    configCapProd.getString("database"),
    configCapProd.getString("user"),
    configCapProd.getString("password")
  )
}
