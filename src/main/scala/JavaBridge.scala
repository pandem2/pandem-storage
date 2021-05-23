package eu.pandem.storage 

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object JavaBridge {
  def getSparkSession(cores:Int = 0) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = 
      SparkSession.builder()
        .master(s"local[${if(cores == 0) "*" else cores.toString}]")
        .config("spark.sql.files.ignoreCorruptFiles", true)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .appName("epitweetr")
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
