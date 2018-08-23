package rdf.spark.etl.lookups

import rdf.spark.etl.framework.operator.Lookup
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class DiaDescOrigl()(implicit conf: Config, spark: SparkSession) extends Lookup{

	override def lookupProcess(dataframe: DataFrame): DataFrame = {
		dataframe.createOrReplaceTempView("od")
		spark.sql("SELECT od.*, (CASE WHEN DIA_TEXT_ACUTE is not null THEN  DIA_TEXT_ACUTE WHEN DIA_TEXT_ACUTE is null and DIA_TEXT_REPEAT is not null THEN NORM(DIA_TEXT_REPEAT) WHEN DIA_TEXT_ACUTE is null and DIA_TEXT_REPEAT is null and SICK_NOTE_DIAGNOSIS is not null THEN SICK_NOTE_DIAGNOSIS WHEN DIA_TEXT_ACUTE is null and DIA_TEXT_REPEAT is null and SICK_NOTE_DIAGNOSIS is null THEN HOSP_DIAGNOSIS_TEXT ELSE DIA_TEXT_ACUTE) as DIA_DESC_ORIGL_LOOKUP")
			.drop("DIA_DESC_ORIGL")
			.withColumnRenamed("DIA_DESC_ORIGL_LOOKUP", "DIA_DESC_ORIGL")
	}

}

