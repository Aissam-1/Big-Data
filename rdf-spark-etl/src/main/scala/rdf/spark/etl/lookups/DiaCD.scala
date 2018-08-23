package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class DiaCD()(implicit conf: Config, spark: SparkSession) extends Lookup{
	
	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		spark.sql("SELECT od.*, (CASE WHEN ICD10_ACUTE is not null THEN ICD10_ACUTE WHEN ICD10_ACUTE is null and ICD10_REPEAT is not null THEN ICD10_REPEAT WHEN ICD10_ACUTE is null and ICD10_REPEAT is null THEN SICK_NOTE_ICD10 ELSE ICD10_ACUTE END) as DIA_CD_LOOKUP")
			.drop("DIA_CD")
			.withColumnRenamed("DIA_CD_LOOKUP", "DIA_CD")
	}
	
}
