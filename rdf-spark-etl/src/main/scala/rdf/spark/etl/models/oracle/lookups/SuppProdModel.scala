package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SuppProdModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{
	
	def getPrID(): DataFrame = {
		super.request("(SELECT DISTINCT PRD_ID, SRC_PROD_CD, SRC_PACK_SIZE, SRC_PROD_LNG_TXT_ORIGL FROM RWE_COMMON.TH_SUPP_PROD) TH_SUPP_PROD")
	}
	
	def getProdSeqNo(): DataFrame = {
		super.request("(SELECT DISTINCT PROD_SEQ_NO, SRC_PROD_CD, SRC_PACK_SIZE, SRC_PROD_LNG_TXT_ORIGL FROM RWE_COMMON.TH_SUPP_PROD_ORIGL) TH_SUPP_PROD_ORIGL")
	}
	
}
