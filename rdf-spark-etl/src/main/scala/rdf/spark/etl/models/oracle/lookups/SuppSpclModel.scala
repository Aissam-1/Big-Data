package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SuppSpclModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{
	
	def getSpeID(): DataFrame = {
		super.request("(SELECT DISTINCT SPE_ID, SRC_SPCL_TEXT FROM RWE_COMMON.TH_SUPP_SPCL) TH_SUPP_SPCL")
	}
	
	def getSpeSeqNo(): DataFrame = {
		super.request("(SELECT DISTINCT SPCL_SEQ_NO, SRC_SPCL_TEXT_ORIGL FROM RWE_COMMON.TH_SUPP_SPCL_ORIGL) TH_SUPP_SPCL_ORIGL")
	}
	
	def getPreSpeId(): DataFrame = {
		super.request("(SELECT DISTINCT PRE_SPE_ID, SRC_SPCL_TEXT FROM RWE_COMMON.TH_SUPP_SPCL) TH_SUPP_SPCL")
	}
	
}
