package com.wcs.mds.jco

import com.sap.conn.jco._
import com.sap.conn.jco.ext.{DestinationDataEventListener, DestinationDataProvider, Environment}
import java.util.Properties
import com.typesafe.config.ConfigFactory
import scala.collection.immutable.HashMap

object JCoService {

  val app = "mds"
  val config = ConfigFactory.load()
  val props = new Properties()
  props.put("jco.client.ashost", config.getString("jco.client.ashost"))
  props.put("jco.client.client", config.getString("jco.client.client"))
  props.put("jco.client.user", config.getString("jco.client.user"))
  props.put("jco.client.passwd", config.getString("jco.client.passwd"))
  props.put("jco.client.lang", config.getString("jco.client.lang"))
  props.put("jco.client.sysnr", config.getString("jco.client.sysnr"))

  val provider = JCoProvider(app, props)
  Environment.unregisterDestinationDataProvider(provider)
  Environment.registerDestinationDataProvider(provider)

  def getFunction(functionName: String): JCoFunction = {
    JCoDestinationManager.getDestination(app).getRepository().getFunctionTemplate(functionName).getFunction()
  }

  def execute(function: JCoFunction): JCoParameterList = {
    function.execute(JCoDestinationManager.getDestination("mds"))
    function.getExportParameterList()
  }
}

case class JCoProvider(key: String, props: Properties) extends DestinationDataProvider {
  val configs = HashMap[String, Properties](key -> props)
  def getDestinationProperties(desName: String): Properties = {
    return configs.getOrElse(desName, new Properties())
  }
  def setDestinationDataEventListener(arg0: DestinationDataEventListener) {}
  def supportsEvents(): Boolean = false
}