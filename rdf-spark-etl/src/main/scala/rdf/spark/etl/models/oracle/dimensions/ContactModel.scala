package rdf.spark.etl.models.oracle.dimensions

import rdf.spark.etl.framework.lib.Conf
import rdf.spark.etl.framework.model.oracle.{ODModel, Oracle}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class ContactModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.od) with ODModel{
  
    override def load: DataFrame = {
        this.request("(SELECT * FROM RWE_ODH.ODH_CONTACT WHERE TRA_ID=710 AND ROWNUM<1000) foo")
    }
    
    override def save(data: DataFrame): Unit = {
    
    }
    
    override def reject(data: DataFrame): Unit = {
    
    }
  
}