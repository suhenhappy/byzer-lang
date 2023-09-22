package streaming.core.strategy.platform

import java.util
import java.util.Properties

import tech.mlsql.dsl.adaptor.MLMapping

import scala.collection.mutable

object ConfigReader {
  val mapping: mutable.Map[String, String] = MLMapping.mapping

  val fileName = "config.properties"

  def init(): Unit = {
    PlatformManager.pluginMapping.foreach { f => mapping.put(f._1, f._2)}
//    init(fileName)
  }

  def init(fileName: String): Unit = {
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName)
    val properties = new Properties()
    properties.load(is)
    val keys : util.Set[String]= properties.keySet().asInstanceOf[util.Set[String]]
    keys.forEach(key => mapping += (key -> properties.getProperty(key)))
  }
}
