package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DmpModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{

    def requestLookup(): DataFrame = {
        super.request("(SELECT DISTINCT DMP_ID, DMP_CD FROM RWE_DE.TH_DMP) TH_DMP")
    }

}
