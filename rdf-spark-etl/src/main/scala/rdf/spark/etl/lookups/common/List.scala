package rdf.spark.etl.lookups.common

import java.util.NoSuchElementException

import rdf.spark.etl.models.oracle.lookups.ListModel
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

trait List {
    
    def getListCategory(listCode: String)(implicit conf: Config, spark: SparkSession): (Any, Any) = {
        try {
            val listModel = new ListModel()
            val item: Row = listModel.requestListCategory(listCode).first()
            (item.get(0), item.get(1))
        }catch{
            case e: NoSuchElementException => (null, null)
        }
        
    }
    
    def getSuppList(listCode: String)(implicit conf: Config, spark: SparkSession): DataFrame = {
        val listModel = new ListModel()
        val cat: (Any, Any) = this.getListCategory(listCode)
        listModel.requestSuppList(cat._1, cat._2)
    }
    
    
    
    
}
