package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{
	
	def getTstID(): DataFrame = {
		super.request("(SELECT DISTINCT TST_UNIT_ID, TEST_UNIT_CD FROM RWE_COMMON.TH_TEST_UNIT) TH_TEST_UNIT")
	}
	
}
