package rdf.spark.etl.lookups.common

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class InputMap(obj: Map[String, String])()(implicit conf: Config, spark: SparkSession) extends Lookup{
  
    override def lookupProcess(dataframe: DataFrame): DataFrame = {
        dataframe.createOrReplaceTempView("od")
        if(obj.isEmpty) {
            dataframe
        } else {
            val modify: Array[String] = this.getMapColumns(obj)
            val remain: Array[String] = this.getStableColumn(modify, dataframe.columns)
            spark.sql("SELECT " + remain.mkString(", ") + ", " + modify.map(x => "'" + obj(x) + "' AS " + x).mkString(", ") + " FROM od")
        }
    }
  
    private def getMapColumns(obj: Map[String, String]): Array[String] = {
        obj.keys.toArray
    }
  
    private def getStableColumn(maps: Array[String], dataframe: Array[String]): Array[String] = {
        dataframe.diff(maps)
    }
  
}
