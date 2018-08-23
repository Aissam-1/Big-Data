package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SuppTestModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{
	
	def getTstID(): DataFrame = {
		super.request("(SELECT DISTINCT TST_ID, SRC_TST_TEXT, SRC_VALUE_TYPE, SRC_TEST_ABBR FROM RWE_COMMON.TH_SUPP_TEST) TH_SUPP_TEST")
	}
	
	def getTstSeqNo(): DataFrame = {
		super.request("(SELECT DISTINCT TST_SEQ_NO, SRC_TST_TEXT, SRC_VALUE_TYPE, SRC_TEST_ABBR FROM RWE_COMMON.TH_SUPP_TEST_ORIGL) TH_SUPP_TEST_ORIGL")
	}
	
}
