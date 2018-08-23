package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class Spe()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		spark.sql("SELECT od.*, NORM(REFERRAL_TO_SPE) as REFERRAL_TO_SPE_LOOKUP")
			.drop("REFERRAL_TO_SPE")
			.withColumnRenamed("REFERRAL_TO_SPE_LOOKUP", "REFERRAL_TO_SPE")
	}
	
}