package rdf.spark.etl.models.oracle.lookups

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import rdf.spark.etl.framework.lib.Conf
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}

class ContactModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{

    def getContact(): DataFrame = {
        super.request("(SELECT DISTINCT DOC_ID, CON_DATE, PAT_ID, CON_ID FROM RWE_DE.TH_CONTACT) TH_CONTACT")
    }

}
