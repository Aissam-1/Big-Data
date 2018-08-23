package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProdLngTxt()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		spark.sql("SELECT od.*, NORM(DRUG_ADDITIONAL_INFO_2) as DRUG_ADDITIONAL_INFO_2_LOOKUP")
			.drop("DRUG_ADDITIONAL_INFO_2")
			.withColumnRenamed("DRUG_ADDITIONAL_INFO_2_LOOKUP", "DRUG_ADDITIONAL_INFO_2")
	}
	
}