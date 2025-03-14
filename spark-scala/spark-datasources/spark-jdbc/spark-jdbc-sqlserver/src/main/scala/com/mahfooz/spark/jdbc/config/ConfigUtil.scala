package com.mahfooz.spark.jdbc.config

import com.typesafe.config.{Config, ConfigFactory}

import java.util.Properties

object ConfigUtil {

  def getOdbcProperty(config: Config): Properties = {
    val url=config.getString("db.url")
    val userName=config.getString("db.userName")
    val password=config.getString("db.password")
    val driver=config.getString("db.driver")

    val props = new java.util.Properties
    props.setProperty("url",url)
    props.setProperty("user",userName)
    props.setProperty("password",password)
    props.setProperty("driver",driver)
    props
  }

  def getOdbcProperty(configFile: String): Properties = {
    val config = ConfigFactory.load(configFile)
    getOdbcProperty(config)
  }

}
