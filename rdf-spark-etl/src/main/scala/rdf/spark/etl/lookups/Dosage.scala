package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class Dosage()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		spark.sql("SELECT od.*, NORM(DOSAGE) as DOSAGE_LOOKUP")
			.drop("DOSAGE")
			.withColumnRenamed("DOSAGE_LOOKUP", "DOSAGE")
	}

}