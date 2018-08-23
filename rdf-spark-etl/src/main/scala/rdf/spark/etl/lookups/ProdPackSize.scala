package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProdPackSize()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		spark.sql("SELECT od.*, NORM(DRUG_ADDITIONAL_INFO_3) as DRUG_ADDITIONAL_INFO_3_LOOKUP")
			.drop("DRUG_ADDITIONAL_INFO_3")
			.withColumnRenamed("DRUG_ADDITIONAL_INFO_3_LOOKUP", "DRUG_ADDITIONAL_INFO_3")
	}

}