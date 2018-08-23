package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DoctorPracticeModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{
    
    def requestLookup(): DataFrame = {
        this.request("(SELECT DISTINCT PRA_EID, ENCRYTED_PRA_EID, DOC_EID FROM RWE_DE.TH_DOCTOR_PRACTICE) TH_DOCTOR_PRACTICE")
    }
    
}
