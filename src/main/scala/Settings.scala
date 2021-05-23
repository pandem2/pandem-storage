package eu.pandem.storage;

import spray.json._
import java.nio.file.Paths

case class Settings(home:String) {
  var _properties:Option[JsObject] = None
  def load() = {
    val psource = scala.io.Source.fromFile(Paths.get(home, "properties.json").toString)
    val plines = try psource.mkString finally psource.close() 
    _properties = Some(plines.parseJson.asJsObject)
    this
  }

  def properties = _properties match {
    case Some(p) => p
    case None => throw new Exception("Please call load() before calling properties on these settings")
  } 

  def sparkCores = properties.fields.get("spark_cores").map(v => v.asInstanceOf[JsNumber].value.toInt)
  def sparkMemory =  properties.fields.get("spark_memory").map(v => v.asInstanceOf[JsString].value)
  def winutils =  System.getProperty("os.name").toLowerCase.contains("windows") match {
    case true => Some(Paths.get(this.home, "hadoop").toString)
    case fakse => None
  }
  def collectionPath =  Paths.get(home, "collections").toString 
  def geolocationThreshold = properties.fields.get("geolocation_threshold").map(v => v.asInstanceOf[JsString].value.toInt)
  def geolocationStrategy = Some("demy.mllib.index.PredictStrategy")
  def fsRoot = Paths.get(home, "fs").toString
  def fsBatchTimeout = properties.fields.get("fs.batch.timeout").map(v => v.asInstanceOf[JsNumber].value.toInt).getOrElse(60*60)
  def fsQueryTimeout = properties.fields.get("fs.query.timeout").map(v => v.asInstanceOf[JsNumber].value.toInt).getOrElse(60)
  def fsPort = properties.fields.get("fs.port").map(v => v.asInstanceOf[JsNumber].value.toInt).getOrElse(8080)
}
