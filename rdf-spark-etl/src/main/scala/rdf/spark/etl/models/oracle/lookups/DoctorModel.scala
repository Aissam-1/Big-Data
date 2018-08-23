package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DoctorModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{

    def requestLookup(): DataFrame = {
        super.request("(SELECT DISTINCT DOC_EID, DOC_ID FROM RWE_DE.TH_DOCTOR) TH_DOCTOR")
    }

}
