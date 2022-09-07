package jotting.task

import com.typesafe.config.ConfigFactory

import jotting.Jotting

object Common {
  private val config        = ConfigFactory.load(Jotting.configFile).getConfig("taskCommon")
  private val configCapRaw  = config.getConfig("capRaw")
  private val configCapProd = config.getConfig("capProd")

  val connCapRaw  = Jotting.Conn(configCapRaw)
  val connCapProd = Jotting.Conn(configCapProd)
}
