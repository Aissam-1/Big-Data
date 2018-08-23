package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProdCd()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		spark.sql("SELECT od.*, NORM(DRUG_ADDITIONAL_INFO_1) as DRUG_ADDITIONAL_INFO_1_LOOKUP")
			.drop("DRUG_ADDITIONAL_INFO_1")
			.withColumnRenamed("DRUG_ADDITIONAL_INFO_1_LOOKUP", "DRUG_ADDITIONAL_INFO_1")
	}
	
}