package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DiaModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{

    def requestLookup(): DataFrame = {
        super.request("(SELECT DISTINCT DIA_ID, SRC_DIAG_CD, SRC_DIAG_CERTAINTY_CD, SRC_DIAG_LOC_CD, SRC_DIAG_DEG_CD, SRC_DIAG_DESC_ORIGL, SRC_DIAG_EXPLN_TXT_ORIGL FROM RWE_COMMON.TH_SUPP_DIAG) TH_SUPP_DIAG")
    }

}
