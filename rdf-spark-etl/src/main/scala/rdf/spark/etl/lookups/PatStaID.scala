package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import rdf.spark.etl.lookups.common.List

class PatStaID()(implicit conf: Config, spark: SparkSession) extends Lookup with List {

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		val lookup: DataFrame = this.getSuppList("MORTAL")
		lookup.createOrReplaceTempView("lookup")

		spark.sql("SELECT od.*, cast(lookup.SUPP_LIST_ID as bigint) as SUPP_LIST_ID_LOOKUP FROM od left outer join lookup on (lookup.SRC_LIST_CD=od.PAT_STA_ID)")
			.drop("PAT_STA_ID")
			.withColumnRenamed("SUPP_LIST_ID_LOOKUP", "PAT_STA_ID")
	}

}
