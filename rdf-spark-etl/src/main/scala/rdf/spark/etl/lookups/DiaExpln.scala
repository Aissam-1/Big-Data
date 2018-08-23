package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class DiaExpln()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		spark.sql("SELECT od.*, NORM(DIA_EXPLANATION) as DIA_EXPLANATION_LOOKUP")
			.drop("DIA_EXPLANATION")
			.withColumnRenamed("DIA_EXPLANATION_LOOKUP", "DIA_EXPLANATION")
	}
	
}