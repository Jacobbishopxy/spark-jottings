package jotting

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import jotting.Jotting._

object TestReadConfig extends App {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  log.info("loading config file...")

  val config = ConfigFactory.load(configFile).getConfig("job")

  val conn = Conn(config)

  log.info("config file loaded: ")
  log.info(conn)

  log.info("done")
}
